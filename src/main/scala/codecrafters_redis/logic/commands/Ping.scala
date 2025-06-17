package codecrafters_redis.logic.commands

import java.io.OutputStream

object Ping extends CommandDetectTrait with CommandHandler {

  override def canHandle(command: String): Boolean = {
    command.toLowerCase.startsWith("ping")
  }

  def handle(out: OutputStream): Unit = {
    val answer = "+PONG\r\n"
    out.write(stringToBytes(answer))
  }
}
