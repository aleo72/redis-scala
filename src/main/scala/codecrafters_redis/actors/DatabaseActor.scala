package codecrafters_redis.actors

import akka.NotUsed
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import akka.actor.typed.{ActorRef, Behavior}
import akka.stream.IOResult
import codecrafters_redis.CmdArgConfig
import codecrafters_redis.rdb.{RedisKeyValue, RedisRdbFile}
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.collection.immutable.Map

object DatabaseActor extends database.KeysTrait {

  val DatabaseKey = akka.actor.typed.receptionist.ServiceKey[CommandOrResponse]("DatabaseActor")

  enum InternalCommand:
    case InitializationSuccess(db: Database)
    case InitializationFailure(exception: Throwable)

  enum Command:
    case Get(key: String, replyTo: ActorRef[Response])
    case Set(key: String, value: Option[Array[Byte]], expired: Option[Long], replyTo: ActorRef[Response])
    case Keys(pattern: String, replyTo: ActorRef[Response])
    case Config(get: Option[String], set: Option[(String, String)], replyTo: ActorRef[Response])

  enum Response:
    case Value(value: Option[Array[Byte]])
    case ValueBulkString(values: Seq[Array[Byte]])
    case Cleared
    case Ok
    case Error(message: String)

  type CommandOrResponse = Command | InternalCommand

  type Database = Map[String, (Array[Byte], Option[Long])]

  def apply(cmdArgConfig: CmdArgConfig): Behavior[CommandOrResponse] =
    Behaviors.setup { ctx =>
      ctx.log.info("Creating DatabaseActor")
      Behaviors.withStash(1000) { buffer =>
        val dbFuture = loadRdbFile(cmdArgConfig)(using ctx)
        ctx.pipeToSelf(dbFuture) {
          case Success(db)        => InternalCommand.InitializationSuccess(db)
          case Failure(exception) => InternalCommand.InitializationFailure(exception)
        }
        initializing(buffer, cmdArgConfig)
      }
    }

  def initializing(
      buffer: StashBuffer[CommandOrResponse],
      cmdArgConfig: CmdArgConfig
  ): Behavior[CommandOrResponse] =
    Behaviors.receive { (ctx: ActorContext[CommandOrResponse], message: CommandOrResponse) =>
      message match {
        case InternalCommand.InitializationSuccess(db) =>
          ctx.log.info("Database initialized successfully.")
          val behavior: Behavior[CommandOrResponse] = handler(db, cmdArgConfig)
          buffer.unstashAll(behavior)

        case InternalCommand.InitializationFailure(exception) =>
          ctx.log.error(s"Failed to initialize database: ${exception.getMessage}")
          Behaviors.stopped

        case msg: CommandOrResponse =>
          ctx.log.warn("Received unexpected message during initialization.")
          buffer.stash(msg)
          Behaviors.same
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

  private def loadRdbFile(cmdArgConfig: CmdArgConfig)(using ctx: ActorContext[CommandOrResponse]): Future[Database] = {

    val filePath: Option[String] = for {
      realDir <- cmdArgConfig.dir
      dbFileName <- cmdArgConfig.dbfilename
    } yield s"$realDir/$dbFileName"

    if (filePath.isEmpty) {
      Future.successful(Map.empty[String, (Array[Byte], Option[Long])])
    } else {
      loadRdbFileFromDisk(filePath.get)
    }
  }

  private def loadRdbFileFromDisk(filePath: String)(using ctx: ActorContext[CommandOrResponse]): Future[Database] = {
    given ec: scala.concurrent.ExecutionContext = ctx.executionContext
    given system: akka.actor.typed.ActorSystem[Nothing] = ctx.system

    val path = java.nio.file.Paths.get(filePath)
    if (!java.nio.file.Files.exists(path)) {
      ctx.log.warn(s"RDB file not found at path: $filePath")
      return Future.successful(Map.empty[String, (Array[Byte], Option[Long])])
    } else {
      ctx.log.info(s"Loading RDB file from path: $filePath")
    }

    val source: Source[ByteString, Future[IOResult]] = akka.stream.scaladsl.FileIO.fromPath(java.nio.file.Paths.get(filePath))
    val rdbFlow: Flow[ByteString, RedisKeyValue, NotUsed] = RedisRdbFile.rdbParserFlow()
    source
      .via(rdbFlow)
      .runFold(Map.empty[String, (Array[Byte], Option[Long])]) { (db, redisKeyValue) =>
        ctx.log.info(s"Parsed key: ${redisKeyValue.key}, value: ${redisKeyValue.value}")
        val expiry = redisKeyValue.expireAt
        db + (redisKeyValue.key -> (redisKeyValue.value.toString.getBytes, expiry))
      }
  }

}
