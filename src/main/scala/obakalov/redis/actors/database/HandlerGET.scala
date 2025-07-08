package obakalov.redis.actors.database

import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors

trait HandlerGET {
  self: DatabaseBehaviourContextTrait =>

  import obakalov.redis.actors.DatabaseActor.*

  def handlerGET(
      cmd: Command.Get
  ): Behavior[CommandOrResponse] = {
    context.log.info(s"Getting value for key: ${cmd.key}")
    val value: Option[Array[Byte]] = store
      .get(cmd.key)
      .filter { case (_, expiry) => expiry.forall(_ > System.currentTimeMillis()) }
      .map(_._1)
    cmd.replyTo ! Response.Value(value)
    Behaviors.same[CommandOrResponse]
  }

}
