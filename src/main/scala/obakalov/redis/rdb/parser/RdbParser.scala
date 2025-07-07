package obakalov.redis.rdb.parser

import obakalov.redis.rdb.models.{RdbValue, RedisEntry}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.util.ByteString

import java.nio.charset.StandardCharsets
import scala.util.control.NonFatal

object RdbParser {

  private val ASCII = StandardCharsets.US_ASCII
  private val UTF_8 = StandardCharsets.UTF_8

  val VERSION_MIN = 1
  val VERSION_MAX = 12

  enum ParsingConsume:
    case CONTINUE
    case WAIT_FOR_MORE

  case class ParserIntermediateResultType(state: ParserState, entry: Option[RedisEntry], isContinue: ParsingConsume)
  object ParserIntermediateResultType {
    def waitForMore(state: ParserState): ParserIntermediateResultType =
      ParserIntermediateResultType(state, None, ParsingConsume.WAIT_FOR_MORE)
  }

  def parserFlow(): Flow[ByteString, RedisEntry, NotUsed] =
    Flow[ByteString].statefulMapConcat { () =>
      var state = ParserState(ParsingStep.ReadingHeader)

      (chunk: ByteString) => {
        state = state.append(chunk)
        var results = Vector.empty[RedisEntry]
        var isNeedToContinue: ParsingConsume = ParsingConsume.CONTINUE
        while (ParsingConsume.CONTINUE == isNeedToContinue) {
          try {
            val ParserIntermediateResultType(newState, result, canContinue) = readNext(state)
            state = newState
            result.foreach(results :+= _)
            isNeedToContinue = canContinue
          } catch {
            case NonFatal(e) =>
              println(s"!!! PARSING FAILED at offset ~${state.bytesConsumed} !!!")
              println(s"    Error: ${e.getMessage}")
              println(s"    Buffer (first 32 bytes): ${state.buffer.take(32).map(b => f"$b%02X").mkString(" ")}")
              println(s"    Buffer: ${state.buffer}")
              throw e // Останавливаем поток
          }
        }
        results
      }
    }

  def readNext(state: ParserState): ParserIntermediateResultType = {
    state.step match {
      case ParsingStep.ReadingHeader =>
        parseHeader(state)
      case ParsingStep.ReadingOpCode | ParsingStep.ReadOpCode =>
        parseOptCode(state)
    }
  }

  private def parseOptCode(state: ParserState): ParserIntermediateResultType = {
    if (state.buffer.isEmpty) {
      ParserIntermediateResultType.waitForMore(state) // Not enough data to read the opcode
    } else {
      val (newState, optCode) = updateOpCodeInState(state)
      val internalResult: ParserIntermediateResultType = optCode match {
        case OpCode.EOF                 => parseEOF(newState)
        case OpCode.SELECT_DB           => parseSelectDb(newState)
        case OpCode.RESIZE_DB           => parseResizeDb(newState)
        case OpCode.AUX                 => parseAuxField(newState)
        case o @ OpCode.MODULE_AUX      => throw new UnsupportedOperationException(s"Redis ${o} are not supported")
        case o @ OpCode.FUNCTION_PRE_GA => throw new UnsupportedOperationException(s"Redis ${o} are not supported")
        case o @ OpCode.FUNCTION_PRE_GA => throw new UnsupportedOperationException(s"Redis ${o} are not supported")
        case o @ OpCode.FUNCTION2       => throw new UnsupportedOperationException(s"Redis ${o} are not supported")
        case o @ OpCode.SLOT_INFO       => throw new UnsupportedOperationException(s"Redis ${o} are not supported")
        case OpCode.EXPIRETIME          => parseExpireTime(newState, isMs = false)
        case OpCode.EXPIRETIME_MS       => parseExpireTime(newState, isMs = true)
        case OpCode.ENTRY_KEY_VALUE     => parseKeyValueEntry(newState)
      }
      internalResult
    }
  }

  private def updateOpCodeInState(state: ParserState): (ParserState, OpCode) = {
    state.step match {
      case s: ParsingStep.ReadOpCode => state -> s.optCode
      case ParsingStep.ReadingOpCode =>
        val byte: Byte = state.buffer.head
        val unsignedInt: Int = byte & 0xff
        val optCode: OpCode = OpCode.fromCode(unsignedInt)
        state.consume(1, ParsingStep.ReadOpCode(optCode)) -> optCode
      case _ => throw new IllegalStateException("Expected to be in ReadingOpCode step with an OptCode")
    }
  }

  private def parseExpireTime(state: ParserState, isMs: Boolean): ParserIntermediateResultType =
    val readBytes = if (isMs) 8 else 4 // 8 bytes for milliseconds, 4 bytes for seconds
    if (state.buffer.length < readBytes) {
      ParserIntermediateResultType.waitForMore(state)
    } else {
      state.buffer.take(readBytes).iterator.getLong(using java.nio.ByteOrder.BIG_ENDIAN) match {
        case expireTime if expireTime < 0 =>
          throw new IllegalArgumentException(s"Invalid expire time: $expireTime")
        case expireTime =>
          val newState = state.consume(readBytes, ParsingStep.ReadingOpCode).copy(nextExpiry = Some(expireTime))
          ParserIntermediateResultType(newState, None, ParsingConsume.CONTINUE)
      }
    }

  private def parseKeyValueEntry(state: ParserState): ParserIntermediateResultType = {
    for {
      (key, stateAfterKey) <- readString(state)
      (value, stateAfterValue) <- readStringEncoded(stateAfterKey)
    } yield {
      val entry = RedisEntry.RedisKeyValue(
        key = key,
        value = RdbValue.RdbValue(value),
        expireAt = stateAfterValue.nextExpiry,
        dbNumber = stateAfterValue.currentDb
      )
      val newState = stateAfterValue.copy(
        step = ParsingStep.ReadingOpCode,
        nextExpiry = None // Reset next expiry as it is not applicable for key-value pairs
      )
      ParserIntermediateResultType(newState, Option(entry), ParsingConsume.CONTINUE)
    }
  }.getOrElse(ParserIntermediateResultType.waitForMore(state))

  private def parseAuxField(state: ParserState): ParserIntermediateResultType = (
    for {
      (key, stateAfterKey) <- readString(state)
      (value, stateAfterValue) <- readString(stateAfterKey)
    } yield {
      val newState = stateAfterValue.consume(0, ParsingStep.ReadingOpCode)
      val entry = RedisEntry.AuxField(key, value)
      ParserIntermediateResultType(newState, Option(entry), ParsingConsume.CONTINUE)
    }
  ).getOrElse(ParserIntermediateResultType.waitForMore(state))

  private def parseResizeDb(state: ParserState): ParserIntermediateResultType = (
    for {
      (dbHashSize, stateAfterHashSize) <- readLength(state)
      (dbExpiresSize, stateAfterExpiresSize) <- readLength(stateAfterHashSize)
    } yield {
      val newState = stateAfterExpiresSize.consume(0, ParsingStep.ReadingOpCode)
      val entry = RedisEntry.ResizeDb(
        dbNumber = newState.currentDb,
        dbHashTableSize = dbHashSize,
        expireTimeHashTableSize = dbExpiresSize
      )
      ParserIntermediateResultType(newState, Option(entry), ParsingConsume.CONTINUE)
    }
  ).getOrElse(ParserIntermediateResultType.waitForMore(state))

  private def parseSelectDb(state: ParserState): ParserIntermediateResultType =
    readLength(state) match {
      case None =>
        ParserIntermediateResultType.waitForMore(state) // Not enough data to read the length
      case Some((value, newState)) =>
        if (value < 0 || value > 255) {
          throw new IllegalArgumentException(s"Invalid database number: $value")
        }
        val dbNumber = value.toInt
        ParserIntermediateResultType(newState.copy(currentDb = dbNumber, step = ParsingStep.ReadingOpCode), None, ParsingConsume.CONTINUE)
    }

  private def readLength(state: ParserState): Option[(Long, ParserState)] =
    if (state.buffer.isEmpty) {
      None // Not enough data to read the length
    } else {
      val firstByte = state.buffer.head & 0xff
      val flag = (firstByte & 0xc0) >> 6
      if (flag == 0) {
        // 00|xxxxxx -> 6-bit length number. The last 6 bits of this byte represent the length.
        val length = firstByte & 0x3f
        val newState = state.consume(1, ParsingStep.ReadingOpCode)
        Option((length, newState))
      } else if (flag == 1) {
        // 01|xxxxxx -> 14-bit length number. The last 6 bits of this byte and the next byte represent the length.
        if (state.buffer.length < 2) {
          None // Not enough data to read the length
        } else {
          val firstPart = firstByte & 0x3f
          val secondByte = state.buffer.slice(1, 2).head
          val secondPart = secondByte & 0xff
          Option(
            ((firstPart << 8) | secondPart, state.consume(2, ParsingStep.ReadingOpCode))
          )
        }
      } else if (firstByte == 0x80) {
        // 10|000000 -> 32-bit length number. Integer is represented by the next 4 bytes.
        if (state.buffer.length < 5) {
          None // Not enough data to read the length
        } else {
          val bytes = state.buffer.slice(1, 5)
          val length = bytes.iterator.getInt(using java.nio.ByteOrder.BIG_ENDIAN)
          Option((length.toLong, state.consume(1 + 4, ParsingStep.ReadingOpCode)))
        }
      } else if (firstByte == 0x81) {
        // 10|000001 -> 32-bit length number with a special flag
        val length32bit = 8 + 1 // 8 bytes for the length + 1 byte for the flag
        if (state.buffer.length < length32bit) {
          None // Not enough data to read the length
        } else {
          val bytes = state.buffer.slice(1, length32bit)
          val length = bytes.iterator.getLong(using java.nio.ByteOrder.BIG_ENDIAN)
          Option((length, state.consume(length32bit, ParsingStep.ReadingOpCode)))
        }
      } else {
        // 11|xxxxxx -> special case, not a length
        throw new RuntimeException("Special case 11xxxxxx is not a length")
      }
    }

  private def readString(state: ParserState): Option[(String, ParserState)] =
    readStringEncoded(state).map(x => String(x._1, UTF_8) -> x._2)

  private def readStringEncoded(state: ParserState): Option[(Array[Byte], ParserState)] =
    if (state.buffer.isEmpty) {
      None
    } else {
      val firstByte = state.buffer.head & 0xff
      val flag = (firstByte & 0xc0) >> 6
      flag match {
        case 0 => // 00|xxxxxx -> 6-bit value of length string
          val lengthString = firstByte & 0x3f
          if (state.buffer.length < lengthString + 1) {
            None // Not enought data to read the string
          } else {
            val stringBytes = state.buffer.slice(1, lengthString + 1).toArray
            val newState = state.consume(lengthString + 1, ParsingStep.ReadingOpCode)
            Some((stringBytes, newState))
          }
        case 1 => // 01|xxxxxx -> 14-bit value of length string
          val lengthValue = 2
          if (state.buffer.length < lengthValue) {
            None // not enough data to read the length
          } else {
            val firstPart = firstByte & 0x3f
            val secondByte = state.buffer.slice(1, 2).head
            val secondPart = secondByte & 0xff
            val lengthString = (firstPart << 8) | secondPart
            extractStringEncoded(state, lengthString, lengthValue)
          }
        case 2 => // 10|xxxxxx -> 32-bit value of length string
          val lengthValue = 5 // 1 byte for flag + 4 bytes for length
          if (state.buffer.length < lengthValue) {
            None // Not enough data to read the length
          } else {
            val bytes = state.buffer.slice(1, lengthValue)
            val lengthString = bytes.iterator.getInt(using java.nio.ByteOrder.BIG_ENDIAN)
            extractStringEncoded(state, lengthString, lengthValue)
          }
        case 3 => // 11|xxxxxx -> special case, not a length
          readSpecialStringEncoded(state.consume(1), firstByte & 0x3f)
        case _ => throw new RuntimeException(s"Invalid flag $flag for string length")
      }
    }

  private def readSpecialStringEncoded(state: ParserState, stringType: Int): Option[(Array[Byte], ParserState)] =
    stringType match {
      case 0 => // read integer 8bit as string
        if (state.buffer.isEmpty) {
          None // Not enough data to read the string
        } else {
          val stringBytes = String.valueOf(state.buffer.head).getBytes
          val newState = state.consume(1, ParsingStep.ReadingOpCode)
          Some((stringBytes, newState))
        }
      case 1 => // read integer 16bit as string
        if (state.buffer.length < 2) {
          None // Not enough data to read the string
        } else {
          val stringBytes = String.valueOf(state.buffer.take(2).iterator.getShort(using java.nio.ByteOrder.BIG_ENDIAN)).getBytes
          val newState = state.consume(2, ParsingStep.ReadingOpCode)
          Some((stringBytes, newState))
        }
      case 2 => // read integer 32bit as string
        if (state.buffer.length < 4) {
          None // Not enough data to read the string
        } else {
          val stringBytes = String.valueOf(state.buffer.take(4).iterator.getInt(using java.nio.ByteOrder.BIG_ENDIAN)).getBytes
          val newState = state.consume(4, ParsingStep.ReadingOpCode)
          Some((stringBytes, newState))
        }
      case 3 => // read Lzf compressed string
        readLzfCompressedString(state.consume(1))
    }

  private def readLzfCompressedString(state: ParserState): Option[(Array[Byte], ParserState)] =
    for {
      (sourceLength, stateAfterSourceLength) <- readLength(state)
      (compressedLength, stateAfterCompressedLength) <- readLength(stateAfterSourceLength)
      if stateAfterCompressedLength.buffer.length >= sourceLength
    } yield {
      val compressedBytes: ByteString = stateAfterCompressedLength.buffer.take(compressedLength.toInt)
      val decompressed = Lzf.decompress(compressedBytes).getBytes
      decompressed -> stateAfterCompressedLength.consume(sourceLength.toInt, ParsingStep.ReadingOpCode) // Placeholder for LZF decompression
    }

  private def extractStringEncoded(state: ParserState, lengthString: Int, lengthValue: Int): Option[(Array[Byte], ParserState)] = {
    if (state.buffer.length < lengthString + lengthValue) {
      None // Not enough data to read the string
    } else {
      val stringBytes = state.buffer.slice(lengthValue, lengthString + lengthValue).toArray
      val newState = state.consume(lengthString + lengthValue, ParsingStep.ReadingOpCode)
      Some((stringBytes, newState))
    }
  }

  private def parseEOF(state: ParserState): ParserIntermediateResultType = {
    val eofCodeLength = 1 // EOF is a single byte
    // checkf if version more then 5 then we can read checksum else empty
    if (state.version >= 5) {
      val checksumLength = 8 // Checksum is 8 bytes
      if (state.buffer.length < eofCodeLength + checksumLength) {
        ParserIntermediateResultType.waitForMore(state) // Not enough data to read the checksum
      } else {
        val checksum = state.buffer.drop(eofCodeLength).take(checksumLength)
        val newState = state.consume(eofCodeLength + checksumLength, ParsingStep.Finished(checksum))
        ParserIntermediateResultType(newState, None, ParsingConsume.CONTINUE)
      }
    } else {
      // If the version is less than 5, we don't read a checksum
      ParserIntermediateResultType(state.consume(eofCodeLength, ParsingStep.Finished(ByteString.empty)), None, ParsingConsume.CONTINUE)
    }
  }

  def parseHeader(state: ParserState): ParserIntermediateResultType = {
    val magicLength = 5
    val versionLength = 4
    val headerLength = magicLength + versionLength

    if (state.buffer.length < headerLength) {
      ParserIntermediateResultType(state, None, ParsingConsume.WAIT_FOR_MORE) // Not enough data to read the header
    } else {
      val magic: String = state.buffer.take(magicLength).decodeString(ASCII)
      if (magic != "REDIS") throw new IllegalArgumentException(s"Invalid RDB magic: $magic")
      val versionBytes: String = state.buffer.slice(magicLength, headerLength).utf8String
      val versionBytesIntMaybe = versionBytes.toIntOption
      versionBytesIntMaybe match {
        case Some(version) if version >= VERSION_MIN && version <= VERSION_MAX =>
          val newState = state
            .consume(headerLength, ParsingStep.ReadingOpCode)
            .setVersion(version)
          ParserIntermediateResultType(newState, None, ParsingConsume.CONTINUE)
        case _ =>
          throw new IllegalArgumentException(
            s"Invalid RDB version $versionBytesIntMaybe: ${versionBytes}"
          )
      }
    }
  }
}
