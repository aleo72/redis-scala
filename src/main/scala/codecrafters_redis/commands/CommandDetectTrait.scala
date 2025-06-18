package codecrafters_redis.commands

import codecrafters_redis.commands.logic.PingLogic.commandName

trait CommandDetectTrait {

  def commandName: String

  def canHandle(command: ProtocolMessage): Boolean = {
    command.bulkMessageString.equalsIgnoreCase(commandName) ||
    command.multiBulkMessage.flatMap(_.headOption).exists(_.bulkMessageString.equalsIgnoreCase(commandName))
  }

}
