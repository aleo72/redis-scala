package codecrafters_redis.rdb

import codecrafters_redis.rdb.ParsingPhase.*
import codecrafters_redis.rdb.RdbOpCode.*
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.util.ByteString

import java.nio.ByteOrder
import scala.util.control.NonFatal

object RedisRdbFile {

  private type ParsingIntermediateResult = (ParserState, Option[RedisKeyValue], Boolean)

  def rdbParserFlow(): Flow[ByteString, RedisKeyValue, org.apache.pekko.NotUsed] =
    Flow[ByteString].statefulMapConcat { () =>
      var state = ParserState()

      (chunk: ByteString) => {
        state = state.append(chunk)
        var results = Vector.empty[RedisKeyValue]
        var continueParsing = true
        while (continueParsing) {
          try {
            val (newState, result, canContinue) = runParserStep(state)
            state = newState
            result.foreach(results :+= _)
            continueParsing = canContinue
          } catch {
            case NonFatal(e) =>
              println(s"!!! PARSING FAILED at offset ~${state.bytesConsumed} !!!")
              println(s"    Error: ${e.getMessage}")
              println(s"    Buffer (first 32 bytes): ${state.buffer.take(32).map(b => f"$b%02X").mkString(" ")}")
              throw e // Останавливаем поток
          }
        }
        results
      }
    }

  private def runParserStep(state: ParserState): ParsingIntermediateResult =
    state.phase match {
      case ReadingHeader                       => parseHeader(state)
      case ReadingOpCode                       => parseOpCode(state)
      case ReadingKeyValuePair(valueTypeCodee) => parseKeyValuePair(state, valueTypeCodee)
      case Finished                            => (state, None, false)
    }

  private def parseKeyValuePair(state: ParserState, valueTypeCode: Int): ParsingIntermediateResult = {
    parseString(state.buffer) match {
      case None                             => (state, None, false)
      case Some((keyBytes, bufferAfterKey)) =>
        parseValue(valueTypeCode, bufferAfterKey) match {
          case None                               => (state, None, false)
          case Some((rdbValue, bufferAfterValue)) =>
            val result = RedisKeyValue(keyBytes.utf8String, rdbValue, state.nextExpiry)
            val bytesConsumed = state.buffer.length - bufferAfterValue.length
            val newState = state.consume(bytesConsumed, ReadingOpCode).copy(nextExpiry = None)
            (newState, Some(result), true)
        }
    }
  }
  private def parseValue(valueTypeCode: Int, buffer: ByteString): Option[(RdbValue, ByteString)] = {
    valueTypeCode match {
      case STRING.code =>
        parseString(buffer).map { case (value, remaining) => (RdbValue.RdbString(value.utf8String), remaining) }
      case ENC_INT8.code =>
        if (buffer.length < 1) None else Some((RdbValue.RdbInt(buffer.head.toLong), buffer.drop(1)))
      case ENC_INT16.code =>
        if (buffer.length < 2) None else Some((RdbValue.RdbInt(buffer.iterator.getShort(using ByteOrder.LITTLE_ENDIAN)), buffer.drop(2)))
      case ENC_INT32.code =>
        if (buffer.length < 4) None else Some((RdbValue.RdbInt(buffer.iterator.getInt(using ByteOrder.LITTLE_ENDIAN)), buffer.drop(4)))
      case _ =>
        throw new RuntimeException(s"Parsing for value type code 0x${valueTypeCode.toHexString} is not implemented yet.")
    }
  }

  private def parseString(buffer: ByteString): Option[(ByteString, ByteString)] = {
    val firstByte = buffer.headOption.map(_.toInt & 0xff).getOrElse(-1)
    if ((firstByte >> 6) == 0x03) // Special case 11xxxxxx
      throw new RuntimeException("Special case 11xxxxxx is not a length")
    ParsedLength.parse(buffer).flatMap { parsedLength =>
      val totalLength = parsedLength.value + parsedLength.consumeBytes
      if (buffer.length >= totalLength) {
        val stringValue = buffer.slice(parsedLength.consumeBytes, totalLength)
        val remainingBuffer = buffer.drop(totalLength)
        Some((stringValue, remainingBuffer)) // Return the parsed string and the remaining buffer
      } else {
        None // Not enough data to read the string
      }
    }
  }

  private def parseEncodedInteger(buffer: ByteString, size: Int): Option[(Int, ByteString)] =
    if (buffer.length < size) None
    else {
      val value = size match {
        case 1 => buffer.head.toLong
        case 2 => buffer.iterator.getShort(using ByteOrder.LITTLE_ENDIAN).toLong
        case 4 => buffer.iterator.getInt(using ByteOrder.LITTLE_ENDIAN).toLong
        case _ => throw new RuntimeException(s"Unsupported size for encoded integer: $size")
      }
      val remainingBuffer = buffer.drop(size)
      Some((value.toInt, remainingBuffer)) // Return as Int, since RDB uses 32-bit integers
    }

  private def parseHeader(state: ParserState): ParsingIntermediateResult =
    if (state.buffer.length < 9) {
      (state, None, false) // Not enough data to read the header
    } else {
      val header = state.buffer.take(9)
      val magic = header.take(5).utf8String
      val version = header.drop(5).take(4).utf8String
      if (magic != "REDIS") {
        throw new RuntimeException(s"Invalid RDB file magic: $magic")
      }
      println(s"RDB file version: $version")
      val newState = state.copy(
        phase = ParsingPhase.ReadingOpCode,
        buffer = state.buffer.drop(9)
      )
      (newState, None, true)
    }

  private def parseOpCode(state: ParserState): ParsingIntermediateResult =
    if (state.buffer.isEmpty) {
      (state, None, false) // Not enough data to read the opcode
    } else {
      val opCode: Int = state.buffer.head.toInt & 0xff // Convert to unsigned int
      val bufferAfterOpCode: ByteString = state.buffer.drop(1)

      opCode match {
        case EOF.code =>
          (state.consume(1, Finished), None, false) // End of file, transition to Finished phase
        case SELECT_DB.code =>
          ParsedLength.parse(state.buffer.drop(1)) match {
            case None         => (state, None, false) // Not enough data to read the DB number
            case Some(length) =>
              val dbNumber = length.value
              val bytesToConsume = length.consumeBytes + 1 // +1 for the opcode byte
              val newState = state
                .consume(bytesToConsume, ReadingOpCode)
                .copy(currentDb = dbNumber, nextExpiry = None)
              (newState, None, true) // Successfully parsed SELECT_DB
          }
        case EXPIRETIME_MS.code =>
          if (bufferAfterOpCode.length < 9) {
            (state, None, false) // Not enough data to read the expiry time, 8 bytes needed for Long
          } else {
            val expiryTime = bufferAfterOpCode.iterator.getLong(using ByteOrder.LITTLE_ENDIAN)
            val newState = state
              .consume(9, ReadingOpCode)
              .copy(
                nextExpiry = Some(expiryTime) // Already in milliseconds
              )
            (newState, None, true) // Successfully parsed EXPIRETIME_MS
          }
        case EXPIRETIME.code =>
          if (bufferAfterOpCode.length < 4) {
            (state, None, false) // Not enough data to read the expiry time, 4 bytes needed for Int
          } else {
            val expiryTime = bufferAfterOpCode.iterator.getInt(using ByteOrder.LITTLE_ENDIAN)
            val newState = state
              .consume(4, ReadingOpCode)
              .copy(
                nextExpiry = Some(expiryTime * 1000L) // Convert seconds to milliseconds
              )
            (newState, None, true) // Successfully parsed EXPIRETIME
          }
        case RESIZE_DB.code | AUX.code =>
          parseString(bufferAfterOpCode)
            .flatMap { case (_, bufferAfterKey) =>
              skipValue(bufferAfterKey).map { bufferAfterValue =>
                val bytesConsumed = state.buffer.length - bufferAfterValue.length
                (state.consume(bytesConsumed, ReadingOpCode), None, true)
              }
            }
            .getOrElse((state, None, false))
        case valueTypeCodee =>
          (state.copy(phase = ReadingKeyValuePair(valueTypeCodee), buffer = bufferAfterOpCode), None, true) // Proceed to read key-value pair

      }
    }

  private def skipValue(buffer: ByteString): Option[ByteString] = {
    if (buffer.isEmpty) return None
    val firstByte = buffer.head.toInt & 0xff

    // Проверяем, это специально закодированное значение (начинается на 11)?
    if ((firstByte >> 6) == 0x03) {
      val bufferAfterType = buffer.drop(1)
      firstByte match {
        case ENC_INT8.code  => if (bufferAfterType.length >= 1) Some(bufferAfterType.drop(1)) else None
        case ENC_INT16.code => if (bufferAfterType.length >= 2) Some(bufferAfterType.drop(2)) else None
        case ENC_INT32.code => if (bufferAfterType.length >= 4) Some(bufferAfterType.drop(4)) else None
        case ENC_LZF.code   =>
          (for {
            compLen <- ParsedLength.parse(bufferAfterType)
            uncompLen <- ParsedLength.parse(bufferAfterType.drop(compLen.consumeBytes))
          } yield {
            val headerLen = compLen.consumeBytes + uncompLen.consumeBytes
            val bytesToSkip = headerLen + compLen.value
            if (bufferAfterType.length >= bytesToSkip) Some(bufferAfterType.drop(bytesToSkip)) else None
          }).flatten
        case _ => throw new RuntimeException(s"Cannot skip unknown special value type 0x${firstByte.toHexString}")
      }
    } else {
      // Иначе это обычная строка с префиксом длины
      ParsedLength.parse(buffer).flatMap { len =>
        val bytesToSkip = len.value + len.consumeBytes
        if (buffer.length >= bytesToSkip) Some(buffer.drop(bytesToSkip)) else None
      }
    }
  }
}
