package codecrafters_redis.actors.database

import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import codecrafters_redis.CmdArgConfig
import codecrafters_redis.actors.DatabaseActor
import codecrafters_redis.actors.DatabaseActor.*

import scala.util.matching.Regex

trait KeysTrait {
  def globToRegex(glob: String): String =
    glob
      .replace(".", "\\.")
      .replace("?", ".")
      .replace("*", ".*")
//      .replace("[!", "[^")
//      .replace("[^", "[^")
//      .replace("[", "[")
//      .replace("]", "]")

  def globPredicate(pattern: String): String => Boolean = {
    val regex = globToRegex(pattern)
    val compiled: Regex = regex.r
    s => compiled.matches(s)
  }

  def handlerKeys(
      cmd: DatabaseActor.Command.Keys,
      db: Database,
      cmdArgConfig: CmdArgConfig,
      context: ActorContext[CommandOrResponse]
  ): Behaviors.Receive[CommandOrResponse] = {
    context.log.info(s"Getting keys with pattern: ${cmd.pattern}")
    context.log.info(s"Database size: ${db.size} keys: ${db.keys.mkString(", ")}")
    val keys = db.keys.filter(globPredicate(cmd.pattern))
    context.log.info(s"Found keys: ${keys.mkString(", ")}")
    cmd.replyTo ! Response.ValueBulkString(keys.toSeq.map(_.getBytes))
    handler(db, cmdArgConfig)
  }

}
