package codecrafters_redis.commands

import akka.actor.typed.ActorRef
import codecrafters_redis.actors.DatabaseActor

import java.io.OutputStream
import java.nio.charset.StandardCharsets

trait CommandHandler {

  def handle(command: ProtocolMessage, out: OutputStream, databaseActor: ActorRef[DatabaseActor.Command]): Unit

  def stringToBytes(str: String): Array[Byte] =
    str.getBytes(StandardCharsets.UTF_8)
  
  def responseToBytes(response: String): Array[Byte] = {
    val formattedResponse = response + "\r\n"
    stringToBytes(formattedResponse)
  }

}
