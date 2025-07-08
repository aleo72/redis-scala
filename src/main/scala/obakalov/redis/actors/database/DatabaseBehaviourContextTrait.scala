package obakalov.redis.actors.database
import obakalov.redis.CmdArgConfig
import obakalov.redis.actors.DatabaseActor.CommandOrResponse
import obakalov.redis.actors.DatabaseActor
import scala.collection.concurrent.TrieMap
import org.apache.pekko.actor.typed.scaladsl.ActorContext

trait DatabaseBehaviourContextTrait {
  val store: TrieMap[String, (Array[Byte], Option[Long])]
  val context: ActorContext[DatabaseActor.CommandOrResponse]
  val cmdArgConfig: CmdArgConfig
}
