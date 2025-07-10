package obakalov.redis.actors.client.logic

import obakalov.redis.actors.client.{CommandDetectTrait, SimpleCommandHandler, ExpectedResponseEnum, ProtocolMessage}
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
    queue.offer(ByteString("+PONG\r\n"))
    ExpectedResponseEnum.NoResponse
  }
}
