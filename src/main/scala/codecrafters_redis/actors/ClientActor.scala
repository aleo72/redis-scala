package codecrafters_redis.actors

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{Behavior, Signal}
import codecrafters_redis.commands.*
import codecrafters_redis.util.RedisInputStream

import java.io.{BufferedReader, Closeable, OutputStream}
import java.net.Socket
import java.nio.charset.StandardCharsets
import scala.concurrent.ExecutionContextExecutor

object ClientActor {
  sealed trait Command
  object Command {
    case object Start extends Command
    case object Stop extends Command
  }

  sealed trait Response

  def apply(clientSocket: Socket): Behavior[Command] = Behaviors.setup { ctx =>

    implicit val ec: ExecutionContextExecutor = ctx.system.executionContext
    ctx.log.info(s"Creating ClientActor for client: ${clientSocket.getInetAddress}:${clientSocket.getPort}")
    ctx.self ! Command.Start

    Behaviors
      .receiveMessage[Command] {
        case Command.Start =>
          ctx.log.info("Starting to read commands from client...")
          startReadingClient(clientSocket, Command.Start, ctx)
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
      message: ClientActor.Command,
      context: ActorContext[ClientActor.Command]
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
            processCommand(protocolMessage, outputStream, context)
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

  def closeStreams(s: Closeable, name: String, context: ActorContext[ClientActor.Command]): Unit = {
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
      context: ActorContext[ClientActor.Command]
  ): Unit = {
    // This function can be extended to handle more commands
    context.log.info(s"Processing command: $protocolMessage")
    RedisCommand.values.find(_.logic.canHandle(protocolMessage)) match {
      case Some(redisCommand) =>
        context.log.info(s"Handling command: ${redisCommand.toString}")
        redisCommand.logic.handle(protocolMessage, out)
      case None =>
        handleUnknownCommand(protocolMessage, out, context)
    }
  }

  def handleUnknownCommand(
      protocolMessage: ProtocolMessage,
      out: OutputStream,
      context: ActorContext[ClientActor.Command]
  ): Unit = {
    context.log.info(s"Unknown command: $protocolMessage")
    val response = s"-ERR unknown command '$protocolMessage'\r\n"
  }

}
