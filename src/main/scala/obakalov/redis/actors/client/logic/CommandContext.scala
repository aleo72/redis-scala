package obakalov.redis.actors.client.logic

import obakalov.redis.actors.client.{ExpectedResponseEnum, ProtocolMessage}
import obakalov.redis.actors.ClientActor
import obakalov.redis.actors.DatabaseActor
import obakalov.redis.actors.ReplicationActor
import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.util.ByteString

case class CommandContext(
    msg: ProtocolMessage,
    queue: SourceQueueWithComplete[ByteString],
    databaseActor: ActorRef[DatabaseActor.Command],
    replicationActor: ActorRef[ReplicationActor.Command],
    replyTo: ActorRef[ClientActor.ComandOrResponse],
    log: org.slf4j.Logger
)

trait CommandHandler {
  def handle(context: CommandContext): ExpectedResponseEnum
}