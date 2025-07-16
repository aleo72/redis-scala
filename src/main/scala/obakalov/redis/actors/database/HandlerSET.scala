package obakalov.redis.actors.database
import obakalov.redis.CmdArgConfig
import obakalov.redis.actors.{ClientActor, DatabaseActor}
import obakalov.redis.actors.DatabaseActor.CommandOrResponse
import obakalov.redis.actors.database.DatabaseBehaviourContextTrait
import obakalov.redis.rdb.models.RdbValue.RdbBinary
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}

import scala.collection.concurrent.TrieMap

trait HandlerSET {
  self: DatabaseBehaviourContextTrait =>

  import obakalov.redis.actors.DatabaseActor.*

  def handlerSET(
      cmd: Command.Set
  ): Behavior[CommandOrResponse] = {
    context.log.info(s"Setting value for key: ${cmd.key}")
    val expiryTime = cmd.expired.map(pxValue => pxValue + System.currentTimeMillis())

    store.update(cmd.db, cmd.key, RdbBinary(cmd.value.getOrElse(Array.emptyByteArray), expiryTime))

    cmd.replyTo ! ClientActor.ExpectingAnswers.Ok
    Behaviors.same[CommandOrResponse]
  }

}
