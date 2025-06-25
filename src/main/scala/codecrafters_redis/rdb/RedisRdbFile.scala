package codecrafters_redis.rdb

import akka.stream.scaladsl.Flow
import akka.util.ByteString
import codecrafters_redis.rdb.ParsingPhase.{Finished, ReadingHeader, ReadingOpCode, ReadingValue}

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
          val (newState, result, canContinue) = state.phase match {
            case ReadingHeader => parseHeader(state)
            case ReadingOpCode => parseOpCode(state)
            case ReadingValue  => parseValue(state)
            case Finished      => (state, None, false)
          }
          state = newState
          result.foreach(results :+= _)
          continueParsing = canContinue
        }
        results
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
      val opcode: Byte = state.buffer.head
      val newState = state.copy(
        phase = ParsingPhase.ReadingValue,
        buffer = state.buffer.drop(1)
      )
      val forMatch = opcode.toInt & 0xff // Convert to unsigned int
      forMatch match {
        case RdbOpCode.SELECTDB.code => ??? // Handle SELECTDB
        case RdbOpCode.EXPIRETIME_MS => ??? // Handle EXPIRETIME_MS
        case RdbOpCode.EXPIRETIME    => ??? // Handle EXPIRETIME
        case RdbOpCode.RESIZEDB      => ??? // Handle RESIZEDB
        case RdbOpCode.AUX.code      => ??? // Handle AUX
        case RdbOpCode.EOF.code      =>
          println("End of RDB file reached.")
          (newState.copy(phase = Finished), None, false) // End of file
        case valueTypeCode => ???
        // Handle other value types
      }
      ???

    }
}
