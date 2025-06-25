package codecrafters_redis.actors

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import codecrafters_redis.CmdArgConfig

import scala.collection.immutable.Map

object DatabaseActor extends database.KeysTrait {

  val DatabaseKey = akka.actor.typed.receptionist.ServiceKey[CommandOrResponse]("DatabaseActor")

  sealed trait Command

  object Command {
    case class Get(key: String, replyTo: ActorRef[Response]) extends Command
    case class Set(key: String, value: Option[Array[Byte]], expired: Option[Long], replyTo: ActorRef[Response]) extends Command
    case class Keys(pattern: String, replyTo: ActorRef[Response]) extends Command
    case class Config(
        get: Option[String],
        set: Option[(String, String)],
        replyTo: ActorRef[Response]
    ) extends Command
  }

  sealed trait Response
  object Response {
    case class Value(value: Option[Array[Byte]]) extends Response
    case class ValueBulkString(values: Seq[Array[Byte]]) extends Response
    case object Cleared extends Response
    case object Ok extends Response
    case class Error(message: String) extends Response
  }

  type CommandOrResponse = Command
  type Database = Map[String, (Array[Byte], Option[Long])]

  def apply(cmdArgConfig: CmdArgConfig): Behavior[CommandOrResponse] =
    Behaviors.setup { ctx =>
      ctx.log.info("Creating DatabaseActor")
      val db = Map.empty[String, (Array[Byte], Option[Long])]
      handler(db, cmdArgConfig)
        .receiveSignal { case (_, akka.actor.typed.PostStop) =>
          ctx.log.info("DatabaseActor stopped.")
          Behaviors.stopped
        }
    }

  def handler(
      db: Database,
      cmdArgConfig: CmdArgConfig
  ): Behaviors.Receive[CommandOrResponse] = Behaviors
    .receive { case (context: ActorContext[CommandOrResponse], command: CommandOrResponse) =>
      command match {
        case cmd: Command.Get    => handlerGET(cmd, db, cmdArgConfig, context)
        case cmd: Command.Set    => handlerSET(cmd, db, cmdArgConfig, context)
        case cmd: Command.Config => handlerConfig(cmd, db, cmdArgConfig, context)
        case cmd: Command.Keys   => handlerKeys(cmd, db, cmdArgConfig, context)

      }
    }

  private def handlerGET(
      cmd: Command.Get,
      db: Database,
      cmdArgConfig: CmdArgConfig,
      context: ActorContext[CommandOrResponse]
  ): Behaviors.Receive[CommandOrResponse] = {
    context.log.info(s"Getting value for key: ${cmd.key}")
    val value: Option[Array[Byte]] = db
      .get(cmd.key)
      .filter { case (_, expiry) => expiry.forall(_ > System.currentTimeMillis()) }
      .map(_._1)
    cmd.replyTo ! Response.Value(value)
    handler(db, CmdArgConfig())
  }

  private def handlerSET(
      cmd: Command.Set,
      db: Database,
      cmdArgConfig: CmdArgConfig,
      context: ActorContext[CommandOrResponse]
  ): Behaviors.Receive[CommandOrResponse] = {
    context.log.info(s"Setting value for key: ${cmd.key}")
    val expiryTime = cmd.expired.map(pxValue => pxValue + System.currentTimeMillis())
    val updatedDb = db + (cmd.key -> (cmd.value.getOrElse(Array.empty[Byte]), expiryTime))
    cmd.replyTo ! Response.Ok
    handler(updatedDb, cmdArgConfig)
  }

  private def handlerConfig(
      cmd: Command.Config,
      db: Database,
      cmdArgConfig: CmdArgConfig,
      context: ActorContext[CommandOrResponse]
  ): Behaviors.Receive[CommandOrResponse] = {
    cmd.get match {
      case Some("dir") =>
        context.log.info(s"Current directory is: ${cmdArgConfig.dir.getOrElse("not set")}")
        val response: Response = cmdArgConfig.dir match {
          case Some(dir) => Response.ValueBulkString(Seq("dir".getBytes, dir.getBytes))
          case None      => Response.Value(None)
        }
        cmd.replyTo ! response
      case Some("dbfilename") =>
        context.log.info(s"Current database filename is: ${cmdArgConfig.dbfilename.getOrElse("not set")}")
        val response = cmdArgConfig.dbfilename match {
          case Some(filename) => Response.ValueBulkString(Seq("dbfilename".getBytes, filename.getBytes))
          case None           => Response.Value(None)
        }
        cmd.replyTo ! response
      case _ =>
        context.log.error(s"Unknown config key: ${cmd.get.getOrElse("unknown")}")
        cmd.replyTo ! Response.Error("Unknown config key")
    }
    handler(db, cmdArgConfig)
  }

}
