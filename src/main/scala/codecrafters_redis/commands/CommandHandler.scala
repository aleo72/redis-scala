package codecrafters_redis.commands

import akka.actor.typed.ActorRef
import akka.stream.scaladsl.SourceQueueWithComplete
import akka.util.ByteString
import codecrafters_redis.actors.{ClientActor, DatabaseActor}

import java.io.OutputStream
import java.nio.charset.StandardCharsets

trait CommandHandler {

  def handle(
      command: ProtocolMessage,
      queue: SourceQueueWithComplete[ByteString],
      databaseActor: ActorRef[DatabaseActor.Command],
      replyTo: ActorRef[DatabaseActor.Response],
      log: org.slf4j.Logger
  ): Unit

  def stringToBytes(str: String): Array[Byte] =
    str.getBytes(StandardCharsets.UTF_8)

  def responseToBytes(response: String): Array[Byte] = {
    val formattedResponse = response + "\r\n"
    stringToBytes(formattedResponse)
  }

}
