package obakalov.redis.actors.client.logic

import obakalov.redis.actors.client.{CommandDetectTrait, ExpectedResponseEnum, ProtocolGenerator, ProtocolMessage, SimpleCommandHandler}
import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.util.ByteString
import obakalov.redis.actors.{ClientActor, DatabaseActor}

import java.io.OutputStream

object PingLogic extends CommandDetectTrait with SimpleCommandHandler {

  override val commandName = "PING"

  def handle(
      command: ProtocolMessage,
      queue: SourceQueueWithComplete[ByteString],
      log: org.slf4j.Logger
  ): ExpectedResponseEnum = {
    command.multiBulkMessage match {
      case Some(multi) if multi.length > 1 =>
        val messageBytes = multi(1).bulkMessage.getOrElse(Array.emptyByteArray)
        queue.offer(ProtocolGenerator.createBulkString(messageBytes))
      case _ =>
        queue.offer(ByteString("+PONG\r\n"))
    }
    ExpectedResponseEnum.NoResponse
  }
}
