package codecrafters_redis.rdb.parser

import org.apache.pekko.util.ByteString

import java.nio.charset.StandardCharsets

object Lzf {

  def decompress(data: ByteString): String = {
    data.decodeString(StandardCharsets.UTF_8) // todo check if this is correct
  }

}
