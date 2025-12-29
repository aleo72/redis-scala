package obakalov.redis.actors.client.logic

import obakalov.redis.actors.client.ProtocolMessage
import obakalov.redis.actors.client.logic.PingLogic.commandName

trait CommandDetectTrait {

  def commandName: String

  def canHandle(command: ProtocolMessage): Boolean = {
    command.bulkMessageString.equalsIgnoreCase(commandName) ||
    command.multiBulkMessage.flatMap(_.headOption).exists(_.bulkMessageString.equalsIgnoreCase(commandName))
  }

}
