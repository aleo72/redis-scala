package codecrafters_redis.commands.logic

import codecrafters_redis.commands.{CommandDetectTrait, CommandHandler, ProtocolMessage}

import java.io.OutputStream

object PingLogic extends CommandDetectTrait with CommandHandler {

  override val commandName = "PING"

  def handle(command: ProtocolMessage, out: OutputStream): Unit = {
    out.write(responseToBytes("+PONG"))
  }
}
