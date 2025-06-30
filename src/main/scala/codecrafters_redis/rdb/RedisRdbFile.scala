package codecrafters_redis.rdb

import akka.stream.scaladsl.Flow
import akka.util.ByteString
import codecrafters_redis.rdb.ParsingPhase.*
import codecrafters_redis.rdb.RdbOpCode.*

import java.nio.ByteOrder

object RedisRdbFile {

  private type ParsingIntermediateResult = (ParserState, Option[RedisKeyValue], Boolean)

  def rdbParserFlow(): Flow[ByteString, RedisKeyValue, akka.NotUsed] =
    Flow[ByteString].statefulMapConcat { () =>
      var state = ParserState()

      (chunk: ByteString) => {
        state = state.append(chunk)
        var results = Vector.empty[RedisKeyValue]
        var continueParsing = true
        while (continueParsing) {
          val (newState, result, canContinue) = runParserStep(state)
          state = newState
          result.foreach(results :+= _)
          continueParsing = canContinue
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

  private def parseKeyValuePair(state: ParserState, valueTypeCodee: Int) = {
    val keyOpt = parseString(state.buffer)
    keyOpt match {
      case None                         => (state, None, false) // Not enough data to read the key
      case Some((key, remainingBuffer)) =>
        val valueOpt = valueTypeCodee match {
          case RdbOpCode.STRING.code => parseString(remainingBuffer)
          case RdbOpCode.LIST.code   => throw new RuntimeException("LIST type not implemented yet")
          case RdbOpCode.SET.code    => throw new RuntimeException("SET type not implemented yet")
          case RdbOpCode.ZSET.code   => throw new RuntimeException("ZSET type not implemented yet")
          case RdbOpCode.HASH.code   => throw new RuntimeException("HASH type not implemented yet")
          case _                     => None // Handle other value types
        }
        valueOpt match {
          case None                            => (state, None, false) // Not enough data to read the value
          case Some((value, bufferAfterValue)) =>
            val result = RedisKeyValue(key.utf8String, RdbValue.RdbString(value.utf8String), state.nextExpiry)
            val newState = state.copy(
              phase = ParsingPhase.ReadingOpCode,
              buffer = bufferAfterValue,
              nextExpiry = None // Reset next expiry after reading a key-value pair
            )
            (newState, Some(result), true) // Successfully parsed a key-value pair
        }
    }
  }

  private def parseString(buffer: ByteString): Option[(ByteString, ByteString)] = ParsedLength.parse(buffer) match {
    case None                                    => None // Not enough data to parse the length
    case Some(ParsedLength(value, consumeBytes)) =>
      val totalLengthForConsume = value + consumeBytes
      if (buffer.length >= totalLengthForConsume) {
        val stringValue = buffer.slice(consumeBytes, totalLengthForConsume)
        val remainingBuffer = buffer.drop(totalLengthForConsume)
        Some((stringValue, remainingBuffer))
      } else {
        None // Not enough data to read the string
      }
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

      val rdbOpCode = RdbOpCode.values.find(_.code == opCode).getOrElse {
        throw new RuntimeException(s"Unknown RDB opcode: $opCode")
      }

      rdbOpCode match {
        case EOF =>
          (state.copy(phase = Finished), None, false) // End of file, stop parsing
        case SELECT_DB =>
          ParsedLength.parse(bufferAfterOpCode) match {
            case None =>
              (state, None, false) // Not enough data to read the DB number
            case Some(ParsedLength(dbNumber, consumeBytes)) =>
              val newState = state.copy(
                phase = ReadingOpCode,
                buffer = bufferAfterOpCode.drop(consumeBytes),
                currentDb = dbNumber
              )
              (newState, None, true) // Successfully parsed SELECT_DB
            case None => (state, None, false) // Not enough data to read the DB number
          }
        case EXPIRETIME_MS =>
          if (bufferAfterOpCode.length < 8) {
            (state, None, false) // Not enough data to read the expiry time, 8 bytes needed for Long
          } else {
            val expiryTime = bufferAfterOpCode.iterator.getLong(using ByteOrder.LITTLE_ENDIAN)
            val newState = state.copy(
              phase = ReadingOpCode,
              buffer = bufferAfterOpCode.drop(8),
              nextExpiry = Some(expiryTime)
            )
            (newState, None, true) // Successfully parsed EXPIRETIME_MS
          }
        case EXPIRETIME =>
          if (bufferAfterOpCode.length < 4) {
            (state, None, false) // Not enough data to read the expiry time, 4 bytes needed for Int
          } else {
            val expiryTime = bufferAfterOpCode.iterator.getInt(using ByteOrder.LITTLE_ENDIAN)
            val newState = state.copy(
              phase = ReadingOpCode,
              buffer = bufferAfterOpCode.drop(4),
              nextExpiry = Some(expiryTime.toLong * 1000) // Convert seconds to milliseconds
            )
            (newState, None, true) // Successfully parsed EXPIRETIME
          }
        case RESIZE_DB | AUX =>
          // this optcode for reading AUX or RESIZEDB and in both cases we need to read a string
          // TODO: handle RESIZEDB properly if needed
          parseString(bufferAfterOpCode).map(_._2).flatMap(parseString) match {
            case None =>
              (state, None, false) // Not enough data to read the AUX or RESIZEDB value
            case Some((value, remainingBuffer)) =>
              val newState = state.copy(
                phase = ReadingOpCode,
                buffer = remainingBuffer
              )
              (newState, None, true) // Successfully parsed AUX or RESIZEDB
          }
        case valueTypeCodee =>
          (state.copy(phase = ReadingKeyValuePair(valueTypeCodee.code), buffer = bufferAfterOpCode), None, true) // Proceed to read key-value pair

      }
    }
}
