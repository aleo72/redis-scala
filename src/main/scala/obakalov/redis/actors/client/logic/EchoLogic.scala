package obakalov.redis.actors.client.logic

import obakalov.redis.actors.client.{CommandDetectTrait, ExpectedResponseEnum, ProtocolGenerator, ProtocolMessage, SimpleCommandHandler}
import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.util.ByteString
import obakalov.redis.actors.{ClientActor, DatabaseActor}

import java.io.OutputStream

object EchoLogic extends CommandDetectTrait with SimpleCommandHandler {

  override def commandName: String = "ECHO"

  override def handle(
      command: ProtocolMessage,
      queue: SourceQueueWithComplete[ByteString],
      log: org.slf4j.Logger
  ): ExpectedResponseEnum = {
    // Extract the message to echo from the command. Redis ECHO expects exactly one argument.
    val messageBytes = command.multiBulkMessage match {
      case Some(multi) if multi.length > 1 =>
        multi(1).bulkMessage.getOrElse(Array.emptyByteArray)
      case _ =>
        Array.emptyByteArray
    }

    queue.offer(ProtocolGenerator.createBulkString(messageBytes))
    ExpectedResponseEnum.NoResponse
  }
}
