package codecrafters_redis.rdb
import org.apache.pekko.util.ByteString

case class ParserState(
    phase: ParsingPhase = ParsingPhase.ReadingHeader,
    buffer: ByteString = ByteString.empty,
    bytesConsumed: Long = 0,
    currentDb: Int = 0,
    nextExpiry: Option[Long] = None
):
  def append(newData: ByteString): ParserState = copy(buffer = buffer ++ newData)

  def consume(bytes: Int, nextPhase: ParsingPhase): ParserState =
    copy(
      phase = nextPhase,
      buffer = this.buffer.drop(bytes),
      bytesConsumed = this.bytesConsumed + bytes
    )
