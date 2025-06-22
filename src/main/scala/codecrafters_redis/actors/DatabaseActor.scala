package codecrafters_redis.actors

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}

object DatabaseActor {

  val DatabaseKey = akka.actor.typed.receptionist.ServiceKey[CommandOrResponse]("DatabaseActor")

  sealed trait Command

  object Command {
    case class Get(key: String, replyTo: ActorRef[Response]) extends Command
    case class Set(key: String, value: String, replyTo: ActorRef[Response]) extends Command
  }

  sealed trait Response
  object Response {
    case class Value(value: Option[String]) extends Response
    case object Cleared extends Response
    case object Ok extends Response
    case class Error(message: String) extends Response
  }

  type CommandOrResponse = Command

  def apply(): Behavior[CommandOrResponse] =
    Behaviors.setup { ctx =>
      ctx.log.info("Creating DatabaseActor")
      Behaviors
        .receiveMessage(handler(ctx, _, Map.empty[String, String]))
        .receiveSignal { case (_, akka.actor.typed.PostStop) =>
          ctx.log.info("DatabaseActor stopped.")
          Behaviors.stopped
        }

    }

  def handler(
      context: ActorContext[CommandOrResponse],
      command: CommandOrResponse,
      db: Map[String, String]
  ): Behavior[CommandOrResponse] =
    command match {
      case Command.Get(key, replyTo) =>
        context.log.info(s"Getting value for key: $key")
        val value = db.get(key)
        replyTo ! Response.Value(value)
        Behaviors.same
      case Command.Set(key, value, replyTo) =>
        context.log.info(s"Setting value for key: $key")
        val updatedDb = db + (key -> value)
        context.log.info(s"Reply answer OK to $replyTo")
        replyTo ! Response.Ok
        Behaviors.receiveMessage { nextCommand =>
          handler(context, nextCommand, updatedDb)
        }
    }

}
