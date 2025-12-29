package obakalov.redis.actors.client.logic

import obakalov.redis.actors.client.{ExpectedResponseEnum, ProtocolMessage}
import obakalov.redis.actors.{ClientActor, DatabaseActor, ReplicationActor}
import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.util.ByteString
import org.slf4j.Logger

object PsyncLogic extends CommandDetectTrait with CommandHandler {
  override def commandName: String = "PSYNC"

  override def handle(cc: CommandContext): ExpectedResponseEnum = {
    cc.log.info(s"Sending $commandName command ${cc.msg} to a replication actor (${cc.replicationActor}) with replyTo: ${cc.replyTo}")
    // Send the PSYNC command to the replication actor
    val messages: Seq[String] = cc.msg.multiBulkMessage.get.drop(1).map(_.bulkMessageString)
    val (runId, offset) = messages match {
      case Seq(rid, off) => (rid, off.toLongOption.getOrElse(-1L))
      case _             => ("?", -1L)
    }
    cc.replicationActor ! ReplicationActor.Command.Psync(cc.replyTo, runId, offset)
    ExpectedResponseEnum.ExpectedResponse
  }
}
