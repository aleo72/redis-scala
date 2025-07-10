package obakalov.redis.actors.client

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.util.ByteString
import obakalov.redis.actors.*

import java.io.OutputStream
import java.nio.charset.StandardCharsets

trait GeneralCommandHandler {

  def stringToBytes(str: String): Array[Byte] =
    str.getBytes(StandardCharsets.UTF_8)

  def responseToBytes(response: String): Array[Byte] = {
    val formattedResponse = response + "\r\n"
    stringToBytes(formattedResponse)
  }
}

trait DatabaseCommandHandler extends GeneralCommandHandler {

  /* return true if no need to wait for a response from the database actor */
  def handle(
      command: ProtocolMessage,
      queue: SourceQueueWithComplete[ByteString],
      databaseActor: ActorRef[DatabaseActor.Command],
      replyTo: ActorRef[DatabaseActor.Response],
      log: org.slf4j.Logger
  ): ExpectedResponseEnum

}

trait SimpleCommandHandler extends GeneralCommandHandler {

  /* return true if no need to wait for a response from the database actor */
  def handle(
      command: ProtocolMessage,
      queue: SourceQueueWithComplete[ByteString],
      log: org.slf4j.Logger
  ): ExpectedResponseEnum

}

trait ReplicationCommandHandler extends GeneralCommandHandler {

  /* return true if no need to wait for a response from the database actor */
  def handle(
      command: ProtocolMessage,
      queue: SourceQueueWithComplete[ByteString],
      databaseActor: ActorRef[DatabaseActor.Command],
      replicationActor: ActorRef[ReplicationActor.Command],
      replyTo: ActorRef[ClientActor.Command],
      log: org.slf4j.Logger
  ): ExpectedResponseEnum

}
