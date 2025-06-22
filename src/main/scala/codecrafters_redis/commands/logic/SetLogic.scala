package codecrafters_redis.commands.logic

import akka.actor.typed.ActorRef
import akka.stream.scaladsl.SourceQueueWithComplete
import akka.util.ByteString
import codecrafters_redis.actors.{ClientActor, DatabaseActor}
import codecrafters_redis.commands.{CommandDetectTrait, CommandHandler, ExpectedResponse, ProtocolMessage}

import java.io.OutputStream

object SetLogic extends CommandDetectTrait with CommandHandler {

  override def commandName: String = "SET"

  override def handle(
      command: ProtocolMessage,
      queue: SourceQueueWithComplete[ByteString],
      databaseActor: ActorRef[DatabaseActor.Command],
      replyTo: ActorRef[DatabaseActor.Response],
      log: org.slf4j.Logger
  ): ExpectedResponse = {
    // Extract the key and value from the command
    command.multiBulkMessage match {
      case Some(multiBulk) if multiBulk.length >= 3 =>
        val key = multiBulk(1).bulkMessageString
        val value = multiBulk(2).bulkMessageString

        log.info(s"Sending SET command to database actor ($databaseActor) with key: $key and value: $value, with replyTo: $replyTo")
        // Send the SET command to the database actor
        databaseActor ! DatabaseActor.Command.Set(key, value, replyTo)
        ExpectedResponse.ExpectedResponse
      case _ =>
        // If the command is malformed, send an error response
//        out.write(responseToBytes("-ERR wrong number of arguments for 'set' command"))
        queue.offer(ByteString("-ERR wrong number of arguments for 'set' command\r\n"))
        ExpectedResponse.NoResponse
    }
  }
}
