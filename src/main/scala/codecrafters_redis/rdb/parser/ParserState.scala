package codecrafters_redis.rdb.parser

import codecrafters_redis.rdb.parser.RdbParser
import org.apache.pekko.util.ByteString

case class ParserState(
    step: ParsingStep,
    version: Int = 0,
    buffer: ByteString = ByteString.empty,
    bytesConsumed: Long = 0,
    currentDb: Int = 0,
    nextExpiry: Option[Long] = None
) {
  def setVersion(newVersion: Int): ParserState =
    copy(version = newVersion)

  def append(newData: ByteString): ParserState = copy(buffer = buffer ++ newData)

  def consume(bytes: Int, nextStep: ParsingStep): ParserState =
    copy(
      step = nextStep,
      buffer = this.buffer.drop(bytes),
      bytesConsumed = this.bytesConsumed + bytes
    )
  def consume(bytes: Int): ParserState =
    copy(
      buffer = this.buffer.drop(bytes),
      bytesConsumed = this.bytesConsumed + bytes
    )
}

enum ParsingStep:
  case ReadingHeader
  case ReadingOpCode
  case Finished(checksum: ByteString)

enum ParsingEntry:
  case EOF
  case SELECT_DB
  case KEY_VALUE_PAIR
  case RESIZE_DB
  case AUX_FIELD
