package obakalov.redis.actors.database
import obakalov.redis.CmdArgConfig
import obakalov.redis.actors.DatabaseActor.CommandOrResponse
import obakalov.redis.actors.DatabaseActor
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}

import scala.collection.concurrent.TrieMap
import obakalov.redis.actors.database.DatabaseBehaviourContextTrait
import org.apache.pekko.actor.typed.Behavior

trait HandlerConfig {
  self: DatabaseBehaviourContextTrait =>

  import obakalov.redis.actors.DatabaseActor.*

  def handlerConfig(
      cmd: Command.Config
  ): Behavior[CommandOrResponse] = {
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
    Behaviors.same[CommandOrResponse]
  }
}
