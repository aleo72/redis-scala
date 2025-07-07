package obakalov.redis.commands.logic

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.util.ByteString
import obakalov.redis.actors.{ClientActor, DatabaseActor}
import obakalov.redis.commands.{CommandDetectTrait, CommandHandler, ExpectedResponse, ProtocolMessage}

import java.io.OutputStream

object PingLogic extends CommandDetectTrait with CommandHandler {

  override val commandName = "PING"

  def handle(
      command: ProtocolMessage,
      queue: SourceQueueWithComplete[ByteString],
      databaseActor: ActorRef[DatabaseActor.Command],
      replyTo: ActorRef[DatabaseActor.Response],
      log: org.slf4j.Logger
  ): ExpectedResponse = {
    queue.offer(ByteString("+PONG\r\n"))
    ExpectedResponse.NoResponse
  }
}
