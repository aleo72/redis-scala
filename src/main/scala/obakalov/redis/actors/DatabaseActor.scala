package obakalov.redis.actors

import obakalov.redis.CmdArgConfig
import obakalov.redis.actors.database.HandlerKeys
import obakalov.redis.actors.database.read.LoadRdbFileTrait
import obakalov.redis.rdb.RedisDataBaseStore
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}

import scala.concurrent.Future
import scala.util.{Failure, Success}

object DatabaseActor {

  val DatabaseKey = org.apache.pekko.actor.typed.receptionist.ServiceKey[CommandOrResponse]("DatabaseActor")

  enum InternalCommand:
    case InitializationSuccess(initStore: RedisDataBaseStore)
    case InitializationFailure(exception: Throwable)

  enum Command:
    case Get(key: String, replyTo: ActorRef[Response], db: Int = 0)
    case Set(key: String, value: Option[Array[Byte]], expired: Option[Long], replyTo: ActorRef[Response], db: Int = 0)
    case Keys(pattern: String, replyTo: ActorRef[Response], db: Int = 0)
    case Config(get: Option[String], set: Option[(String, String)], replyTo: ActorRef[Response])

  enum Response:
    case Value(value: Option[Array[Byte]])
    case ValueBulkString(values: Seq[Array[Byte]])
    case Cleared
    case Ok
    case Error(message: String)

  type CommandOrResponse = Command | InternalCommand

  import obakalov.redis.actors.database.*

  def apply(cmdConfig: CmdArgConfig): Behavior[CommandOrResponse] =
    Behaviors.setup { ctx =>
      ctx.log.info("Creating DatabaseActor")

      val loaderRdbFile = new LoadRdbFileTrait {
        override val context: ActorContext[CommandOrResponse] = ctx
        override val cmdArgConfig: CmdArgConfig = cmdConfig
      }

      Behaviors.withStash(1000) { buffer =>
        val dbFuture: Future[RedisDataBaseStore] = loaderRdbFile.loadRdbFile()
        ctx.pipeToSelf(dbFuture) {
          case Success(value)     => InternalCommand.InitializationSuccess(value)
          case Failure(exception) => InternalCommand.InitializationFailure(exception)
        }
        initializing(buffer, cmdConfig)
      }
    }

  trait DatabaseFullBehaviorTrait extends DatabaseBehaviourContextTrait with HandlerGET with HandlerSET with HandlerKeys with HandlerConfig
  def createDatabaseFullBehaviorTrait(
      ctx: ActorContext[CommandOrResponse],
      storeDatabase: RedisDataBaseStore,
      cmdConfig: CmdArgConfig
  ): DatabaseFullBehaviorTrait = new DatabaseFullBehaviorTrait {
    override val context: ActorContext[CommandOrResponse] = ctx
    override val store: RedisDataBaseStore = storeDatabase
    override val cmdArgConfig: CmdArgConfig = cmdConfig
  }

  def initializing(
      buffer: StashBuffer[CommandOrResponse],
      cmdArgConfig: CmdArgConfig
  ): Behavior[CommandOrResponse] =
    Behaviors.receive { (ctx: ActorContext[CommandOrResponse], message: CommandOrResponse) =>
      message match {
        case InternalCommand.InitializationSuccess(initStore) =>
          ctx.log.info("Database initialized successfully.")
          val databaseFullBehaviors = createDatabaseFullBehaviorTrait(ctx, initStore, cmdArgConfig)
          val behavior: Behavior[CommandOrResponse] = handler(databaseFullBehaviors)
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

  def handler(handlers: DatabaseFullBehaviorTrait): Behaviors.Receive[CommandOrResponse] = Behaviors
    .receive { case (context: ActorContext[CommandOrResponse], command: CommandOrResponse) =>
      command match {
        case cmd: Command.Get    => handlers.handlerGET(cmd)
        case cmd: Command.Set    => handlers.handlerSET(cmd)
        case cmd: Command.Config => handlers.handlerConfig(cmd)
        case cmd: Command.Keys   => handlers.handlerKeys(cmd)
        case cmd                 => throw new IllegalArgumentException(s"Unknown command: $cmd")
      }
    }

}
