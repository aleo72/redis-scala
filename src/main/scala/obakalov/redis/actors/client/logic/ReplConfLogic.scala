package obakalov.redis.actors.client.logic

import obakalov.redis.actors.client.{ExpectedResponseEnum, ProtocolMessage}
import obakalov.redis.actors.{ClientActor, DatabaseActor, ReplicationActor}
import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.util.ByteString
import org.slf4j.Logger

object ReplConfLogic extends CommandDetectTrait with CommandHandler {
  override def commandName: String = "REPLCONF"

  override def handle(cc: CommandContext): ExpectedResponseEnum = {
    cc.log.info(s"Sending $commandName command ${cc.msg} to a replication actor (${cc.replicationActor}) with replyTo: ${cc.replyTo}")
    // Send the REPLCONF command to the replication actor
    val messages: Seq[String] = cc.msg.multiBulkMessage.get.drop(1).map(_.bulkMessageString)
    val params: Map[String, String] = messages.grouped(2).collect { case Seq(key, value) => key -> value }.toMap
    cc.replicationActor ! ReplicationActor.Command.ReplConf(cc.replyTo, params)
    ExpectedResponseEnum.ExpectedResponse
  }
}
