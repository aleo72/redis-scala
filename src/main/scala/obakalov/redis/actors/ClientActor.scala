package obakalov.redis.actors

import obakalov.redis.actors.client.*
import obakalov.redis.actors.client.ExpectedResponseEnum.*
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.util.ByteString

object ClientActor {

  enum Command:
    case ReceivedData(data: ByteString)
    case SendToClient(data: ByteString)
    case Disconnected

  enum ExpectingAnswers:
    case Value(value: Option[Array[Byte]])
    case ValueBulkString(values: Seq[Array[Byte]])
    case Cleared
    case Ok
    case Error(message: String)

  type ComandOrResponse = Command | ExpectingAnswers

  def apply(
      queue: SourceQueueWithComplete[ByteString],
      dbActor: ActorRef[DatabaseActor.Command],
      replicationActor: ActorRef[ReplicationActor.Command]
  ): Behavior[ComandOrResponse] =
    Behaviors.withStash(1000) { buffer => idle(queue, dbActor, replicationActor, buffer) }

  private def idle(
      queue: SourceQueueWithComplete[ByteString],
      dbActor: ActorRef[DatabaseActor.Command],
      replicationActor: ActorRef[ReplicationActor.Command],
      buffer: StashBuffer[ComandOrResponse]
  ): Behavior[ComandOrResponse] = {
    Behaviors.receive { (ctx, message) =>
      message match {
        case Command.ReceivedData(data) =>
          val valueString = data.utf8String.trim
          ctx.log.info(s"Received data from client:\n---start---\n$valueString\n---end---")
          val message: Option[ProtocolMessage] = Protocol.read(data)
          message match {
            case Some(msg) =>
              ctx.log.info(s"Parsed message: $msg")
              processProtocolMessage(msg, queue, dbActor, replicationActor, ctx, buffer)
            case None =>
              ctx.log.error("Failed to parse message from client.")
              Behaviors.same
          }

        case Command.SendToClient(data) =>
          ctx.log.info(s"Sending data to client: ${data.utf8String.trim}")
          queue.offer(data)
          Behaviors.same
        case Command.Disconnected =>
          ctx.log.info("Client disconnected, stopping actor.")
          queue.complete()
          Behaviors.stopped
        case other =>
          // todo not finished
          throw new IllegalArgumentException(s"Unexpected message in idle state: $other")
      }
    }
  }

  private def processProtocolMessage(
      msg: ProtocolMessage,
      queue: SourceQueueWithComplete[ByteString],
      dbActor: ActorRef[DatabaseActor.Command],
      replicationActor: ActorRef[ReplicationActor.Command],
      ctx: ActorContext[ComandOrResponse],
      buffer: StashBuffer[ComandOrResponse]
  ): Behavior[ComandOrResponse] = {
    RedisCommand.values.find(_.logic.canHandle(msg)) match {
      case None =>
        ctx.log.error(s"Unknown command: $msg")
        // Optionally send an error response back to the client
        queue.offer(ByteString(s"-ERR unknown command '${msg.statusCodeString}'\r\n"))
        Behaviors.same
      case Some(redisCommand) =>
        val areYourWaitingResponse: ExpectedResponseEnum = redisCommand match {
          case simpleCommand: RedisSimpleCommand           => simpleCommand.logic.handle(msg, queue, ctx.log)
          case databaseCommand: RedisDatabaseCommand       => databaseCommand.logic.handle(msg, queue, dbActor, ctx.self, ctx.log)
          case replicationCommand: RedisReplicationCommand => replicationCommand.logic.handle(msg, queue, dbActor, replicationActor, ctx.self, ctx.log)
        }
        areYourWaitingResponse match {
          case ExpectedResponse =>
            ctx.log.info("Waiting for response from database actor.")
            working(queue, dbActor, replicationActor, buffer)
          case NoResponse =>
            ctx.log.info("No response expected, returning to idle state.")
            idle(queue, dbActor, replicationActor, buffer)
        }
    }

  }

  private def working(
      queue: SourceQueueWithComplete[ByteString],
      dbActor: ActorRef[DatabaseActor.Command],
      replicationActor: ActorRef[ReplicationActor.Command],
      buffer: StashBuffer[ComandOrResponse]
  ): Behavior[ComandOrResponse] =
    Behaviors.receive { (ctx, message) =>
      message match {
        case ClientActor.ExpectingAnswers.Ok =>
          ctx.log.info("Received OK response from database actor.")
          queue.offer(ByteString("+OK\r\n"))
          buffer.unstashAll(idle(queue, dbActor, replicationActor, buffer))
        case ClientActor.ExpectingAnswers.Value(value) =>
          ctx.log.info(s"Received Value response from database actor: $value")
          value match {
            case Some(v) => queue.offer(ByteString('+') ++ ByteString(v) ++ ByteString("\r\n"))
            case None    => queue.offer(ByteString("$-1\r\n")) // nil response
          }
          buffer.unstashAll(idle(queue, dbActor, replicationActor, buffer))
        case ClientActor.ExpectingAnswers.ValueBulkString(values) =>
          ctx.log.info("Received ValueBulkString response from database actor.")
          val bulkStringResponse: ByteString =
            values
              .map(v => ByteString("$") ++ ByteString(v.length.toString) ++ ByteString("\r\n") ++ ByteString(v) ++ ByteString("\r\n"))
              .foldLeft(
                if (values.isEmpty) ByteString("*0\r\n")
//                else if (values.length == 1) ByteString.empty
                else ByteString("*") ++ ByteString(values.length.toString) ++ ByteString("\r\n")
              )(_ ++ _)
          ctx.log.info(s"Bulk string response: ${bulkStringResponse.utf8String.trim}")
          queue.offer(ByteString(bulkStringResponse))
          buffer.unstashAll(idle(queue, dbActor, replicationActor, buffer))
        case Command.Disconnected =>
          ctx.log.info("Client disconnected, stopping actor.")
          queue.complete()
          Behaviors.stopped
        case other =>
          ctx.log.warn("Working state received unexpected message: {}", other)
          buffer.stash(other)
          Behaviors.same
      }
    }

}
