package codecrafters_redis.actors

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}

import scala.collection.immutable.Map

object DatabaseActor {

  val DatabaseKey = akka.actor.typed.receptionist.ServiceKey[CommandOrResponse]("DatabaseActor")

  sealed trait Command

  object Command {
    case class Get(key: String, replyTo: ActorRef[Response]) extends Command
    case class Set(key: String, value: Option[Array[Byte]], expired: Option[Long], replyTo: ActorRef[Response]) extends Command
  }

  sealed trait Response
  object Response {
    case class Value(value: Option[Array[Byte]]) extends Response
    case object Cleared extends Response
    case object Ok extends Response
    case class Error(message: String) extends Response
  }

  type CommandOrResponse = Command

  def apply(): Behavior[CommandOrResponse] =
    Behaviors.setup { ctx =>
      ctx.log.info("Creating DatabaseActor")
      val db = Map.empty[String, (Array[Byte], Option[Long])]
      handler(db)
        .receiveSignal { case (_, akka.actor.typed.PostStop) =>
          ctx.log.info("DatabaseActor stopped.")
          Behaviors.stopped
        }
    }

  def handler(
      db: Map[String, (Array[Byte], Option[Long])]
  ): Behaviors.Receive[CommandOrResponse] = Behaviors
    .receive { case (context: ActorContext[CommandOrResponse], command: CommandOrResponse) =>
      command match {
        case Command.Get(key, replyTo) =>
          context.log.info(s"Getting value for key: $key")
          val value = db
            .get(key)
            .filter { case (_, expiry) =>
              expiry.forall(_ > System.currentTimeMillis())
            }
            .map(_._1)
          replyTo ! Response.Value(value)
          handler(db)
        case Command.Set(key, value, expired, replyTo) =>
          context.log.info(s"Setting value for key: $key")
          val expiryTime = expired.map(pxValue => pxValue + System.currentTimeMillis())
          val updatedDb = db + (key -> (value.getOrElse(Array.empty[Byte]), expiryTime))
          replyTo ! Response.Ok
          handler(updatedDb)

      }
    }

}
