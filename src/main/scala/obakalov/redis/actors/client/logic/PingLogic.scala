package obakalov.redis.actors.client.logic

import obakalov.redis.actors.client.{ExpectedResponseEnum, ProtocolGenerator, ProtocolMessage}
import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.util.ByteString
import obakalov.redis.actors.{ClientActor, DatabaseActor}

import java.io.OutputStream

object PingLogic extends CommandDetectTrait with CommandHandler {

  override val commandName = "PING"

  def handle(cc: CommandContext): ExpectedResponseEnum = {
    cc.msg.multiBulkMessage match {
      case Some(multi) if multi.length > 1 =>
        val messageBytes = multi(1).bulkMessage.getOrElse(Array.emptyByteArray)
        cc.queue.offer(ProtocolGenerator.createBulkString(messageBytes))
      case _ =>
        cc.queue.offer(ByteString("+PONG\r\n"))
    }
    ExpectedResponseEnum.NoResponse
  }
}
