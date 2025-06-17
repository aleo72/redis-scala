package codecrafters_redis.commands.logic

import codecrafters_redis.commands.{CommandDetectTrait, CommandHandler, ProtocolMessage}

import java.io.OutputStream

object PingLogic extends CommandDetectTrait with CommandHandler {

  override val commandName = "PING"

  override def canHandle(command: ProtocolMessage): Boolean = {
    command.bulkMessageString.equalsIgnoreCase(commandName) ||
    command.multiBulkMessage.flatMap(_.headOption).exists(_.bulkMessageString.equalsIgnoreCase(commandName))
  }

  def handle(command: ProtocolMessage, out: OutputStream): Unit = {
    val answer = "+PONG\r\n"
    out.write(stringToBytes(answer))
  }
}
