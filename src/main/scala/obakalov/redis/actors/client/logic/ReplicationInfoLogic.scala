package obakalov.redis.actors.client.logic

import obakalov.redis.actors.client.{ExpectedResponseEnum, ProtocolMessage}
import obakalov.redis.actors.{ClientActor, DatabaseActor, ReplicationActor}
import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.util.ByteString
import org.slf4j.Logger

object ReplicationInfoLogic extends CommandDetectTrait with CommandHandler {
  override def commandName: String = "INFO"

  override def canHandle(command: ProtocolMessage): Boolean = super.canHandle(command) &&
    command.multiBulkMessage.exists(_.length == 2) &&
    command.multiBulkMessage.get(1).bulkMessageString == "replication"

  override def handle(cc: CommandContext): ExpectedResponseEnum = {
    cc.log.info(s"Sending $commandName command to a replication actor (${cc.replicationActor}) with replyTo: ${cc.replyTo}")
    // Send the INFO command to the replication actor
    cc.replicationActor ! ReplicationActor.Command.Info(cc.replyTo)
    ExpectedResponseEnum.ExpectedResponse
  }
}
