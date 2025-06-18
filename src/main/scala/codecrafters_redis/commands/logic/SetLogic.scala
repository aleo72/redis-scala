package codecrafters_redis.commands.logic

import codecrafters_redis.commands.{CommandDetectTrait, CommandHandler, ProtocolMessage}

import java.io.OutputStream

object SetLogic extends CommandDetectTrait with CommandHandler {

  override def commandName: String = "SET"

  override def handle(command: ProtocolMessage, out: OutputStream): Unit = ???
}
