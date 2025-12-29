package obakalov.redis.actors.client.logic

import obakalov.redis.actors.client.ProtocolMessage
import obakalov.redis.actors.client.logic.PingLogic.commandName

trait CommandDetectTrait {

  def commandName: String

  def canHandle(command: ProtocolMessage): Boolean = {
    command.bulkMessageString.equalsIgnoreCase(commandName) ||
    command.multiBulkMessage.flatMap(_.headOption).exists(_.bulkMessageString.equalsIgnoreCase(commandName))
  }
  
  protected def logCommand(cc: CommandContext): Unit = {
    cc.log.info(s"Sending $commandName command ${cc.msg} to a replication actor (${cc.replicationActor}) with replyTo: ${cc.replyTo}")
  }

}
