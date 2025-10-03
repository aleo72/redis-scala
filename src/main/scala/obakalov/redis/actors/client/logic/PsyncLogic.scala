package obakalov.redis.actors.client.logic

import obakalov.redis.actors.client.{CommandDetectTrait, ExpectedResponseEnum, ProtocolMessage, ReplicationCommandHandler}
import obakalov.redis.actors.{ClientActor, DatabaseActor, ReplicationActor}
import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.util.ByteString
import org.slf4j.Logger

object PsyncLogic extends CommandDetectTrait with ReplicationCommandHandler {
  override def commandName: String = "PSYNC"

  override def handle(
      command: ProtocolMessage,
      queue: SourceQueueWithComplete[ByteString],
      databaseActor: ActorRef[DatabaseActor.Command],
      replicationActor: ActorRef[ReplicationActor.Command],
      replyTo: ActorRef[ClientActor.ExpectingAnswers],
      log: Logger
  ): ExpectedResponseEnum = {
    log.info(s"Sending $commandName command $command to replication actor ($replicationActor) with replyTo: $replyTo")
    // Send the PSYNC command to the replication actor
    val messages: Seq[String] = command.multiBulkMessage.get.drop(1).map(_.bulkMessageString)
    val (runId, offset) = messages match {
      case Seq(rid, off) => (rid, off.toLongOption.getOrElse(-1L))
      case _             => ("?", -1L)
    }
    replicationActor ! ReplicationActor.Command.Psync(replyTo, runId, offset)
    ExpectedResponseEnum.ExpectedResponse
  }
}
