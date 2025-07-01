package codecrafters_redis.rdb

import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.util.ByteString
import codecrafters_redis.rdb.ParsingPhase.*
import codecrafters_redis.rdb.RdbOpCode.*

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
              println(s"    Buffer (first 32 bytes): ${state.buffer.take(32).map(b => f"0x${b.toHexString.toUpperCase}").mkString(" ")}")
              throw e
          }
        }
        results
      }
    }

  private def runParserStep(state: ParserState): ParsingIntermediateResult =
    state.phase match {
      case ReadingHeader                      => parseHeader(state)
      case ReadingOpCode                      => parseOpCode(state)
      case ReadingKeyValuePair(valueTypeCode) => parseKeyValuePair(state, valueTypeCode)
      case Finished                           => (state, None, false)
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
    if (buffer.isEmpty) return None
    val firstByte = buffer.head.toInt & 0xff
    if ((firstByte >> 6) == 0x03) { // Проверяем на 11xxxxxx
      throw new RuntimeException("Logic error: parseString called on a specially encoded value.")
    }
    ParsedLength.parse(buffer).flatMap { parsedLength =>
      val totalLength = parsedLength.value + parsedLength.consumeBytes
      if (buffer.length >= totalLength) {
        val stringValue = buffer.slice(parsedLength.consumeBytes, totalLength)
        val remainingBuffer = buffer.drop(totalLength)
        Some((stringValue, remainingBuffer))
      } else None
    }
  }

  private def parseHeader(state: ParserState): ParsingIntermediateResult = {
    if (state.buffer.length < 9) (state, None, false)
    else (state.consume(9, ReadingOpCode), None, true)
  }

  private def parseOpCode(state: ParserState): ParsingIntermediateResult = {
    if (state.buffer.isEmpty) (state, None, false)
    else {
      val opCode = state.buffer.head.toInt & 0xff
      val bufferAfterOpCode = state.buffer.drop(1)

      opCode match {
        case EOF.code =>
          (state.consume(1, Finished), None, false)

        case RESIZE_DB.code | AUX.code =>
          parseString(bufferAfterOpCode)
            .flatMap { case (_, bufferAfterKey) =>
              skipValue(bufferAfterKey).map { bufferAfterValue =>
                val bytesConsumed = state.buffer.length - bufferAfterValue.length
                (state.consume(bytesConsumed, ReadingOpCode), None, true)
              }
            }
            .getOrElse((state, None, false)) // Если не хватает данных, ждем еще

        case SELECT_DB.code =>
          ParsedLength.parse(bufferAfterOpCode) match {
            case Some(len) =>
              val dbNumber = len.value
              val bytesToConsume = 1 + len.consumeBytes
              val newState = state.consume(bytesToConsume, ReadingOpCode).copy(currentDb = dbNumber)
              (newState, None, true)
            case None => (state, None, false)
          }

        // ... другие мета-команды, например EXPIRETIME_MS ...

        // Если это не мета-команда, считаем ее типом значения
        case valueTypeCode =>
          (state.consume(1, ReadingKeyValuePair(valueTypeCode)), None, true)
      }
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
