package codecrafters_redis.commands.logic

import akka.actor.typed.ActorRef
import akka.stream.scaladsl.SourceQueueWithComplete
import akka.util.ByteString
import codecrafters_redis.actors.DatabaseActor
import codecrafters_redis.commands.{CommandDetectTrait, CommandHandler, ExpectedResponse, ProtocolMessage}
import org.slf4j.Logger

object ConfigLogic extends CommandDetectTrait with CommandHandler {

  override def commandName: String = "CONFIG"

  override def handle(
      command: ProtocolMessage,
      queue: SourceQueueWithComplete[ByteString],
      databaseActor: ActorRef[DatabaseActor.Command],
      replyTo: ActorRef[DatabaseActor.Response],
      log: Logger
  ): ExpectedResponse =
    command.multiBulkMessage match {
      case Some(multiBulk) if multiBulk.length == 3 && multiBulk(1).bulkMessageString.equalsIgnoreCase("GET") =>
        val action = multiBulk(1).bulkMessageString
        val key = multiBulk(2).bulkMessageString
        log.info(s"Sending $commandName command to database actor ($databaseActor) with action: $action, key: $key, with replyTo: $replyTo")

        databaseActor ! DatabaseActor.Command.Config(Option(key), None, replyTo)
        ExpectedResponse.ExpectedResponse
      case _ =>
        // If the command is malformed, send an error response
        queue.offer(ByteString("-ERR wrong number of arguments for 'get' command\r\n"))
        ExpectedResponse.NoResponse
    }
}
