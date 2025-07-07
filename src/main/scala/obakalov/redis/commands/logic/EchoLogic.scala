package obakalov.redis.commands.logic

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.util.ByteString
import obakalov.redis.actors.{ClientActor, DatabaseActor}
import obakalov.redis.commands.{CommandDetectTrait, CommandHandler, ExpectedResponse, ProtocolMessage}

import java.io.OutputStream

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
