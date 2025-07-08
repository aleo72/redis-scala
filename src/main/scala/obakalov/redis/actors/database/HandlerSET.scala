package obakalov.redis.actors.database
import obakalov.redis.actors.DatabaseActor.CommandOrResponse
import obakalov.redis.actors.DatabaseActor
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}

import scala.collection.concurrent.TrieMap
import obakalov.redis.CmdArgConfig
import obakalov.redis.actors.database.DatabaseBehaviourContextTrait
import org.apache.pekko.actor.typed.Behavior

trait HandlerSET {
  self: DatabaseBehaviourContextTrait =>

  import obakalov.redis.actors.DatabaseActor.*

  def handlerSET(
      cmd: Command.Set
  ): Behavior[CommandOrResponse] = {
    context.log.info(s"Setting value for key: ${cmd.key}")
    val expiryTime = cmd.expired.map(pxValue => pxValue + System.currentTimeMillis())

    store.update(cmd.key, (cmd.value.getOrElse(Array.empty[Byte]), expiryTime))

    cmd.replyTo ! Response.Ok
    Behaviors.same[CommandOrResponse]
  }

}
