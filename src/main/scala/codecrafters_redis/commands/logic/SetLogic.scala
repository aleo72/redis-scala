package codecrafters_redis.commands.logic

import akka.actor.typed.ActorRef
import codecrafters_redis.actors.DatabaseActor
import codecrafters_redis.commands.{CommandDetectTrait, CommandHandler, ProtocolMessage}

import java.io.OutputStream

object SetLogic extends CommandDetectTrait with CommandHandler {

  override def commandName: String = "SET"

  override def handle(
      command: ProtocolMessage,
      out: OutputStream,
      databaseActor: ActorRef[DatabaseActor.Command]
  ): Unit = {
    // Extract the key and value from the command
    command.multiBulkMessage match {
      case Some(multiBulk) if multiBulk.length >= 3 =>
        val key = multiBulk(1).bulkMessageString
        val value = multiBulk(2).bulkMessageString

        // Send the SET command to the database actor
        databaseActor ! DatabaseActor.Command.Set(key, value, ???)

      case _ =>
        // If the command is malformed, send an error response
        out.write(responseToBytes("-ERR wrong number of arguments for 'set' command"))
    }
  }
}
