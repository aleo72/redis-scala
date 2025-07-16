package obakalov.redis.actors.database

import obakalov.redis.actors.ClientActor
import obakalov.redis.rdb.models.RdbValue.RdbBinary
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
      .get(cmd.db, cmd.key)
      .map {
        case e: RdbBinary =>
          e.value
        case _ =>
          throw new RuntimeException(s"Unexpected value type for key: ${cmd.key}")
      }

    cmd.replyTo ! ClientActor.ExpectingAnswers.Value(value)
    Behaviors.same[CommandOrResponse]
  }

}
