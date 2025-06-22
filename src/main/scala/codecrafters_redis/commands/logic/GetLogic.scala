package codecrafters_redis.commands.logic

import akka.actor.typed.ActorRef
import akka.stream.scaladsl.SourceQueueWithComplete
import akka.util.ByteString
import codecrafters_redis.actors.DatabaseActor
import codecrafters_redis.commands.{CommandDetectTrait, CommandHandler, ExpectedResponse, ProtocolMessage}
import org.slf4j.Logger

object GetLogic extends CommandDetectTrait with CommandHandler {

  override def commandName: String = "GET"

  override def handle(
      command: ProtocolMessage,
      queue: SourceQueueWithComplete[ByteString],
      databaseActor: ActorRef[DatabaseActor.Command],
      replyTo: ActorRef[DatabaseActor.Response],
      log: Logger
  ): ExpectedResponse =
    command.multiBulkMessage match {
      case Some(multiBulk) if multiBulk.length == 2 =>
        val key = multiBulk(1).bulkMessageString
        log.info(s"Sending GET command to database actor ($databaseActor) with key: $key, with replyTo: $replyTo")
        // Send the GET command to the database actor
        databaseActor ! DatabaseActor.Command.Get(key, replyTo)
        ExpectedResponse.ExpectedResponse
      case _ =>
        // If the command is malformed, send an error response
        queue.offer(ByteString("-ERR wrong number of arguments for 'get' command\r\n"))
        ExpectedResponse.NoResponse
    }
}
