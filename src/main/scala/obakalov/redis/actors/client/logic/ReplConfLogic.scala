package obakalov.redis.actors.client.logic

import obakalov.redis.actors.{ClientActor, DatabaseActor, ReplicationActor}
import obakalov.redis.actors.client.{CommandDetectTrait, ExpectedResponseEnum, ProtocolMessage, ReplicationCommandHandler}
import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.util.ByteString
import org.slf4j.Logger

object ReplConfLogic extends CommandDetectTrait with ReplicationCommandHandler{
  override def commandName: String = "REPLCONF"

  override def handle(command: ProtocolMessage, queue: SourceQueueWithComplete[ByteString], databaseActor: ActorRef[DatabaseActor.Command], replicationActor: ActorRef[ReplicationActor.Command], replyTo: ActorRef[ClientActor.ExpectingAnswers], log: Logger): ExpectedResponseEnum = {
    log.info(s"Sending $commandName command $command to replication actor ($replicationActor) with replyTo: $replyTo")
    // Send the REPLCONF command to the replication actor
    val messages: Seq[String] = command.multiBulkMessage.get.drop(1).map(_.bulkMessageString)
    val params: Map[String, String] = messages.grouped(2).collect { case Seq(key, value) => key -> value}.toMap
    replicationActor ! ReplicationActor.Command.ReplConf(replyTo, params)
    ExpectedResponseEnum.ExpectedResponse
  }
}
