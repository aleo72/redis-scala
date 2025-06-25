package codecrafters_redis.rdb
import akka.util.ByteString

case class ParserState(
    phase: ParsingPhase = ParsingPhase.ReadingHeader,
    buffer: ByteString = ByteString.empty,
    currentDb: Int = 0,
    nextExpiry: Option[Long] = None
):
  def append(newData: ByteString): ParserState = copy(buffer = buffer ++ newData)
