package obakalov.redis.actors.client.logic

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.util.ByteString
import obakalov.redis.actors.DatabaseActor
import obakalov.redis.actors.client.{CommandDetectTrait, DatabaseCommandHandler, ExpectedResponseEnum, ProtocolMessage}
import org.slf4j.Logger

object GetLogic extends CommandDetectTrait with DatabaseCommandHandler {

  override def commandName: String = "GET"

  override def handle(
      command: ProtocolMessage,
      queue: SourceQueueWithComplete[ByteString],
      databaseActor: ActorRef[DatabaseActor.Command],
      replyTo: ActorRef[DatabaseActor.Response],
      log: Logger
  ): ExpectedResponseEnum =
    command.multiBulkMessage match {
      case Some(multiBulk) if multiBulk.length == 2 =>
        val key = multiBulk(1).bulkMessageString
        log.info(s"Sending GET command to database actor ($databaseActor) with key: $key, with replyTo: $replyTo")
        // Send the GET command to the database actor
        databaseActor ! DatabaseActor.Command.Get(key, replyTo)
        ExpectedResponseEnum.ExpectedResponse
      case _ =>
        // If the command is malformed, send an error response
        queue.offer(ByteString("-ERR wrong number of arguments for 'get' command\r\n"))
        ExpectedResponseEnum.NoResponse
    }
}
