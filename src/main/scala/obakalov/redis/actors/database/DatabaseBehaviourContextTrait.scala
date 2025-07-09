package obakalov.redis.actors.database
import obakalov.redis.CmdArgConfig
import obakalov.redis.actors.DatabaseActor.CommandOrResponse
import obakalov.redis.actors.DatabaseActor
import obakalov.redis.rdb.RedisDataBaseStore

import scala.collection.concurrent.TrieMap
import org.apache.pekko.actor.typed.scaladsl.ActorContext

trait DatabaseBehaviourContextTrait {
  val store: RedisDataBaseStore
  val context: ActorContext[DatabaseActor.CommandOrResponse]
  val cmdArgConfig: CmdArgConfig
}
