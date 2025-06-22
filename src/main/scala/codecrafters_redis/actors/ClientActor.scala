package codecrafters_redis.actors

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.scaladsl.SourceQueueWithComplete
import akka.util.ByteString
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

  def apply(queue: SourceQueueWithComplete[ByteString], dbActor: ActorRef[DatabaseActor.Command]): Behavior[ComandOrResponse] = {
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
                  redisCommand.logic.handle(msg, queue, dbActor, ctx.self, ctx.log)
                case None =>
                  ctx.log.error(s"Unknown command: $msg")
                  // Optionally send an error response back to the client
                  queue.offer(ByteString(s"-ERR unknown command '${msg.statusCodeString}'\r\n"))
              }
            case None =>
              ctx.log.error("Failed to parse message from client.")
          }
          ctx.log.info(s"Parsed message: $message")
          Behaviors.same
        case Command.SendToClient(data) =>
          ctx.log.info(s"Sending data to client: ${data.utf8String.trim}")
          queue.offer(data)
          Behaviors.same
        case DatabaseActor.Response.Ok =>
          ctx.log.info("Received OK response from database actor.")
          queue.offer(ByteString("+OK\r\n"))
          Behaviors.same
        case Command.Disconnected =>
          ctx.log.info("Client disconnected, stopping actor.")
          queue.complete()
          Behaviors.stopped
      }
    }
  }

  /*

  def apply(queue: akka.stream.scaladsl.SourceQueueWithComplete[ByteString]): Behavior[ComandOrResponse] =
    Behaviors.setup { ctx =>

      implicit val ec: ExecutionContextExecutor = ctx.system.executionContext
      ctx.log.info(s"Creating ClientActor for client: ${clientSocket.getInetAddress}:${clientSocket.getPort}")
      ctx.self ! Command.Start

      Behaviors
        .receiveMessage[ComandOrResponse] {
          case Command.Start =>
            ctx.log.info("Starting to read commands from client...")
            Future {
              startReadingClient(clientSocket, ctx, databaseActor)
            }(ec)
            Behaviors.same
          case Command.Stop =>
            ctx.log.info("Received Stop command, stopping the actor.")
            closeStreams(clientSocket, "ClientSocket", ctx)
            Behaviors.stopped
        }
        .receiveSignal { case (_, akka.actor.typed.PostStop) =>
          ctx.log
            .info("ClientActor stopped, closing resources.")
          closeStreams(clientSocket, "ClientSocket", ctx)
          Behaviors.stopped
        }

    }

  private def startReadingClient(
      clientSocket: Socket,
      context: ActorContext[ComandOrResponse],
      databaseActor: ActorRef[DatabaseActor.Command]
  ) = {
    val outputStream: OutputStream = clientSocket.getOutputStream
    val ris = new RedisInputStream(clientSocket.getInputStream)
    try {
      context.log.info("Starting to read commands from client...")

      Iterator
        .continually(Protocol.read(ris))
        .takeWhile(_ != null)
        .foreach {
          case Some(protocolMessage) =>
            context.log.info(s"Received command from client: $protocolMessage")
            processCommand(protocolMessage, outputStream, context, databaseActor)
          case None =>
            context.log.info("No more commands to read from client.")
            closeStreams(outputStream, "OutputStream", context)
            closeStreams(ris, "ClientInputStream", context)
            context.self ! ClientActor.Command.Stop // Stop the actor
        }
    } finally {
      closeStreams(outputStream, "OutputStream", context)
      closeStreams(ris, "ClientInputStream", context)
    }
    Behaviors.same
  }

  private def createReader(inputStream: java.io.InputStream): BufferedReader =
    new BufferedReader(
      new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)
    )

  def closeStreams(s: Closeable, name: String, context: ActorContext[ComandOrResponse]): Unit = {
    try {
      if (s != null) {
        s.close()
        context.log.info(s"$name closed successfully.")
      }
    } catch {
      case e: Exception =>
        context.log.error(s"Error closing $name: ${e.getMessage}")
    }
  }

  private def processCommand(
      protocolMessage: ProtocolMessage,
      out: OutputStream,
      context: ActorContext[ComandOrResponse],
      databaseActor: ActorRef[DatabaseActor.Command]
  ): Unit = {
    // This function can be extended to handle more commands
    context.log.info(s"Processing command: $protocolMessage")
    RedisCommand.values.find(_.logic.canHandle(protocolMessage)) match {
      case Some(redisCommand) =>
        context.log.info(s"Handling command: ${redisCommand.toString}")
        redisCommand.logic.handle(protocolMessage, out, databaseActor)
      case None =>
        handleUnknownCommand(protocolMessage, out, context, databaseActor)
    }
  }

  def handleUnknownCommand(
      protocolMessage: ProtocolMessage,
      out: OutputStream,
      context: ActorContext[ComandOrResponse],
      databaseActor: ActorRef[DatabaseActor.Command]
  ): Unit = {
    context.log.info(s"Unknown command: $protocolMessage")
    val response = s"-ERR unknown command '$protocolMessage'\r\n"
  }
   */

}
