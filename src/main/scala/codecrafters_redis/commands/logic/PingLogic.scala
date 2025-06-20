package codecrafters_redis.commands.logic

import akka.actor.typed.ActorRef
import codecrafters_redis.actors.DatabaseActor
import codecrafters_redis.commands.{CommandDetectTrait, CommandHandler, ProtocolMessage}

import java.io.OutputStream

object PingLogic extends CommandDetectTrait with CommandHandler {

  override val commandName = "PING"

  def handle(command: ProtocolMessage, out: OutputStream, databaseActor: ActorRef[DatabaseActor.Command]): Unit = {
    out.write(responseToBytes("+PONG"))
  }
}
