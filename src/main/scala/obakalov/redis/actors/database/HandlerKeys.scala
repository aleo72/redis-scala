package obakalov.redis.actors.database

import obakalov.redis.CmdArgConfig
import obakalov.redis.actors.DatabaseActor.*
import obakalov.redis.actors.{ClientActor, DatabaseActor}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}

import scala.util.matching.Regex

trait HandlerKeys {
  self: DatabaseBehaviourContextTrait =>

  import obakalov.redis.actors.DatabaseActor.*

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
      cmd: DatabaseActor.Command.Keys
  ): Behavior[CommandOrResponse] = {
    context.log.info(s"Getting keys with pattern: ${cmd.pattern}")
    context.log.info(s"Database size: ${store.size} keys: ${store.keys(cmd.db).mkString(", ")}")
    val keys = store.keys(cmd.db).filter(globPredicate(cmd.pattern))
    context.log.info(s"Found keys: ${keys.mkString(", ")}")
    cmd.replyTo ! ClientActor.ExpectingAnswers.ArrayBulkString(keys.toSeq.map(_.getBytes))
    Behaviors.same[CommandOrResponse]
  }

}
