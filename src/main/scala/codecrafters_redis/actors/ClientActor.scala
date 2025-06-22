package codecrafters_redis.actors

import akka.actor.typed.scaladsl.{Behaviors, StashBuffer}
import akka.actor.typed.{ActorRef, Behavior}
import akka.stream.scaladsl.SourceQueueWithComplete
import akka.util.ByteString
import codecrafters_redis.commands.ExpectedResponse.*
import codecrafters_redis.commands.{Protocol, ProtocolMessage, RedisCommand}

object ClientActor {
  sealed trait Command

  object Command {
    case class ReceivedData(data: ByteString) extends Command
    case class SendToClient(data: ByteString) extends Command
    case object Disconnected extends Command
  }

  sealed trait Response

  sealed trait DbResponse extends Response

  object DbResponse {
    case class Value(value: Option[String]) extends DbResponse
    case object Cleared extends DbResponse
    case object Ok extends DbResponse
    case class Error(message: String) extends DbResponse
  }

  type ComandOrResponse = Command | DatabaseActor.Response

  def apply(queue: SourceQueueWithComplete[ByteString], dbActor: ActorRef[DatabaseActor.Command]): Behavior[ComandOrResponse] =
    Behaviors.withStash(1000) { buffer => idle(queue, dbActor, buffer) }

  private def idle(
      queue: SourceQueueWithComplete[ByteString],
      dbActor: ActorRef[DatabaseActor.Command],
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
              RedisCommand.values.find(_.logic.canHandle(msg)) match {
                case Some(redisCommand) =>
                  ctx.log.info(s"Handling command: ${redisCommand.toString}")
                  val areYourWaitingResponse = redisCommand.logic.handle(msg, queue, dbActor, ctx.self, ctx.log)
                  areYourWaitingResponse match {
                    case ExpectedResponse =>
                      ctx.log.info("Waiting for response from database actor.")
                      working(queue, dbActor, buffer)
                    case NoResponse =>
                      ctx.log.info("No response expected, returning to idle state.")
                      idle(queue, dbActor, buffer)
                  }
                case None =>
                  ctx.log.error(s"Unknown command: $msg")
                  // Optionally send an error response back to the client
                  queue.offer(ByteString(s"-ERR unknown command '${msg.statusCodeString}'\r\n"))
                  Behaviors.same
              }
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
      }
    }
  }

  private def working(
      queue: SourceQueueWithComplete[ByteString],
      dbActor: ActorRef[DatabaseActor.Command],
      buffer: StashBuffer[ComandOrResponse]
  ): Behavior[ComandOrResponse] =
    Behaviors.receive { (ctx, message) =>
      message match {
        case DatabaseActor.Response.Ok =>
          ctx.log.info("Received OK response from database actor.")
          queue.offer(ByteString("+OK\r\n"))
          buffer.unstashAll(idle(queue, dbActor, buffer))
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
