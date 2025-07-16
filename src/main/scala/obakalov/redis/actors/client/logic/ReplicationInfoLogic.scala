package obakalov.redis.actors.client.logic

import obakalov.redis.actors.client.{CommandDetectTrait, ExpectedResponseEnum, ProtocolMessage, ReplicationCommandHandler}
import obakalov.redis.actors.{ClientActor, DatabaseActor, ReplicationActor}
import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.util.ByteString
import org.slf4j.Logger

object ReplicationInfoLogic extends CommandDetectTrait with ReplicationCommandHandler {
  override def commandName: String = "INFO"

  override def canHandle(command: ProtocolMessage): Boolean = super.canHandle(command) &&
    command.multiBulkMessage.exists(_.length == 2) &&
    command.multiBulkMessage.get(1).bulkMessageString == "replication"

  override def handle(
      command: ProtocolMessage,
      queue: SourceQueueWithComplete[ByteString],
      databaseActor: ActorRef[DatabaseActor.Command],
      replicationActor: ActorRef[ReplicationActor.Command],
      replyTo: ActorRef[ClientActor.ExpectingAnswers],
      log: Logger
  ): ExpectedResponseEnum = {
    log.info(s"Sending $commandName command to replication actor ($replicationActor) with replyTo: $replyTo")
    // Send the INFO command to the replication actor
    replicationActor ! ReplicationActor.Command.Info(replyTo)
    ExpectedResponseEnum.ExpectedResponse
  }
}
