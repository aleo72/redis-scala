package obakalov.redis.actors.client.logic

import obakalov.redis.actors.{ClientActor, DatabaseActor, ReplicationActor}
import obakalov.redis.actors.client.{CommandDetectTrait, ExpectedResponseEnum, ProtocolMessage, ReplicationCommandHandler, SimpleCommandHandler}
import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.util.ByteString
import org.slf4j.Logger

object ReplicationInfoLogic extends CommandDetectTrait with ReplicationCommandHandler {
  override def commandName: String = "INFO"

  override def handle(
      command: ProtocolMessage,
      queue: SourceQueueWithComplete[ByteString],
      databaseActor: ActorRef[DatabaseActor.Command],
      replicationActor: ActorRef[ReplicationActor.Command],
      replyTo: ActorRef[ClientActor.Command],
      log: Logger
  ): ExpectedResponseEnum = ???
}
