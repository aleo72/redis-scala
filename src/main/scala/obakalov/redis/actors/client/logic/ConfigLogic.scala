package obakalov.redis.actors.client.logic

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.util.ByteString
import obakalov.redis.actors.{ClientActor, DatabaseActor}
import obakalov.redis.actors.client.{CommandDetectTrait, DatabaseCommandHandler, ExpectedResponseEnum, ProtocolMessage}
import org.slf4j.Logger

object ConfigLogic extends CommandDetectTrait with DatabaseCommandHandler {

  override def commandName: String = "CONFIG"

  override def handle(
      command: ProtocolMessage,
      queue: SourceQueueWithComplete[ByteString],
      databaseActor: ActorRef[DatabaseActor.Command],
      replyTo: ActorRef[ClientActor.ExpectingAnswers],
      log: Logger
  ): ExpectedResponseEnum =
    command.multiBulkMessage match {
      case Some(multiBulk) if multiBulk.length == 3 && multiBulk(1).bulkMessageString.equalsIgnoreCase("GET") =>
        val action = multiBulk(1).bulkMessageString
        val key = multiBulk(2).bulkMessageString
        log.info(s"Sending $commandName command to database actor ($databaseActor) with action: $action, key: $key, with replyTo: $replyTo")

        databaseActor ! DatabaseActor.Command.Config(Option(key), None, replyTo)
        ExpectedResponseEnum.ExpectedResponse
      case _ =>
        // If the command is malformed, send an error response
        queue.offer(ByteString("-ERR wrong number of arguments for 'get' command\r\n"))
        ExpectedResponseEnum.NoResponse
    }
}
