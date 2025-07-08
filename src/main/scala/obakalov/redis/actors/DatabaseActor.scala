package obakalov.redis.actors

import obakalov.redis.CmdArgConfig
import obakalov.redis.actors.database.HandlerKeys
import obakalov.redis.actors.database.read.LoadRdbFileTrait
import obakalov.redis.rdb.models.{RdbValue, RedisEntry}
import obakalov.redis.rdb.parser.RdbParser
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.stream.IOResult
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.apache.pekko.util.ByteString

import scala.collection.concurrent.TrieMap
import scala.collection.immutable.Map
import scala.concurrent.Future
import scala.util.{Failure, Success}

object DatabaseActor {

//  val DatabaseKey = org.apache.pekko.actor.typed.receptionist.ServiceKey[CommandOrResponse]("DatabaseActor")
  type Database = TrieMap[String, (Array[Byte], Option[Long])]

  enum InternalCommand:
    case InitializationSuccess(initStore: Database)
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

  import obakalov.redis.actors.database.*

  def apply(cmdArgConfig: CmdArgConfig): Behavior[CommandOrResponse] =
    Behaviors.setup { ctx =>
      ctx.log.info("Creating DatabaseActor")

      val loaderRdbFile = new LoadRdbFileTrait {
        override val context: ActorContext[CommandOrResponse] = ctx
        override val cmdArgConfig: CmdArgConfig = cmdArgConfig
      }

      Behaviors.withStash(1000) { buffer =>
        val dbFuture: Future[Database] = loaderRdbFile.loadRdbFile()
        ctx.pipeToSelf(dbFuture) {
          case Success(value)     => InternalCommand.InitializationSuccess(value)
          case Failure(exception) => InternalCommand.InitializationFailure(exception)
        }
        initializing(buffer, cmdArgConfig)
      }
    }

  trait DatabaseFullBehaviorTrait extends DatabaseBehaviourContextTrait with HandlerGET with HandlerSET with HandlerKeys with HandlerConfig
  def createDatabaseFullBehaviorTrait(
      context: ActorContext[CommandOrResponse],
      store: Database,
      cmdArgConfig: CmdArgConfig
  ): DatabaseFullBehaviorTrait = new DatabaseFullBehaviorTrait {
    override val context: ActorContext[CommandOrResponse] = context
    override val store: Database = store
    override val cmdArgConfig: CmdArgConfig = cmdArgConfig
  }

  def initializing(
      buffer: StashBuffer[CommandOrResponse],
      cmdArgConfig: CmdArgConfig
  ): Behavior[CommandOrResponse] =
    Behaviors.receive { (ctx: ActorContext[CommandOrResponse], message: CommandOrResponse) =>
      message match {
        case InternalCommand.InitializationSuccess(initStore) =>
          ctx.log.info("Database initialized successfully.")
          val databaseFullBehaviors = createDatabaseFullBehaviorTrait(
            context = ctx,
            store = initStore,
            cmdArgConfig = cmdArgConfig
          )
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
      }
    }

}
