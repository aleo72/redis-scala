package codecrafters_redis.commands.logic

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.util.ByteString
import codecrafters_redis.actors.{ClientActor, DatabaseActor}
import codecrafters_redis.commands.{CommandDetectTrait, CommandHandler, ProtocolMessage}

import java.io.OutputStream
import codecrafters_redis.commands.ExpectedResponse

object EchoLogic extends CommandDetectTrait with CommandHandler {

  override def commandName: String = "ECHO"

  override def handle(
      command: ProtocolMessage,
      queue: SourceQueueWithComplete[ByteString],
      databaseActor: ActorRef[DatabaseActor.Command],
      replyTo: ActorRef[DatabaseActor.Response],
      log: org.slf4j.Logger
  ): ExpectedResponse = {
    // Extract the message to echo from the command
    val message = command.multiBulkMessage.map(_.tail).map(_.map(_.bulkMessageString).mkString(" ")).getOrElse("")
    // Write the response to the output stream
    //    out.write(responseToBytes(s"+$message"))
    queue.offer(ByteString(s"+$message\r\n"))
    ExpectedResponse.NoResponse
  }
}
