package codecrafters_redis.commands

import java.io.OutputStream
import java.nio.charset.StandardCharsets

trait CommandHandler {

  def handle(command: ProtocolMessage, out: OutputStream): Unit

  def stringToBytes(str: String): Array[Byte] =
    str.getBytes(StandardCharsets.UTF_8)

}
