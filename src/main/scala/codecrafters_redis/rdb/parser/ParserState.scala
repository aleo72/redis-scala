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

/** Represents the current parsing step in the RDB file. This enum is used to track the state of the parser as it processes the RDB file.
  *   - `ReadingHeader`: The parser is reading the header of the RDB file.
  *   - `ReadingOpCode`: The parser is reading the operation code (OpCode).
  *   - `ReadOpCode(optCode: OptCode)`: The parser has read an OpCode and is now processing it.
  *   - `Finished(checksum: ByteString)`: The parser has finished reading the RDB file and has a checksum.
  */
enum ParsingStep:
  case ReadingHeader
  case ReadingOpCode
  case ReadOpCode(optCode: OptCode)
  case Finished(checksum: ByteString)

enum ParsingEntry:
  case EOF
  case SELECT_DB
  case KEY_VALUE_PAIR
  case RESIZE_DB
  case AUX_FIELD
