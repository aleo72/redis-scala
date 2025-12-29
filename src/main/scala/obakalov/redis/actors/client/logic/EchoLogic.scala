package obakalov.redis.actors.client.logic

import obakalov.redis.actors.client.{ExpectedResponseEnum, ProtocolGenerator, ProtocolMessage}
import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.util.ByteString
import obakalov.redis.actors.{ClientActor, DatabaseActor}

import java.io.OutputStream

object EchoLogic extends CommandDetectTrait with CommandHandler {

  override def commandName: String = "ECHO"

  override def handle(cc: CommandContext): ExpectedResponseEnum = {
    logCommand(cc)
    // Extract the message to echo from the command. Redis ECHO expects exactly one argument.
    val messageBytes = cc.msg.multiBulkMessage match {
      case Some(multi) if multi.length > 1 =>
        multi(1).bulkMessage.getOrElse(Array.emptyByteArray)
      case _ =>
        Array.emptyByteArray
    }

    cc.queue.offer(ProtocolGenerator.createBulkString(messageBytes))
    ExpectedResponseEnum.NoResponse
  }
}
