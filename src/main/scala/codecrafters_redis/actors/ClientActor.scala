package codecrafters_redis.actors

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{Behavior, Signal}
import codecrafters_redis.logic.commands.{CommandDetectTrait, CommandHandler, Echo, Ping, Protocol, ProtocolMessage}
import codecrafters_redis.util.RedisInputStream

import java.io.{BufferedReader, Closeable, OutputStream}
import java.net.Socket
import java.nio.charset.StandardCharsets

object ClientActor {
  sealed trait Command
  object Command {
    case object Start extends Command
  }

  sealed trait Response

  def props(clientSocket: Socket): Behavior[Command] =
    Behaviors.setup { ctx =>
      ctx.log.info(
        s"Creating ClientActor for client: ${clientSocket.getInetAddress}:${clientSocket.getPort}"
      )
      new ClientActor(ctx, clientSocket)
    }
}

class ClientActor(
    context: ActorContext[ClientActor.Command],
    val clientSocket: Socket
) extends AbstractBehavior[ClientActor.Command](context) {

  import ClientActor._

  context.log.info(
    s"ClientActor created for client ${context.self.path}: ${clientSocket.getInetAddress}:${clientSocket.getPort}"
  )

  context.self.tell(Command.Start)

  override def onSignal: PartialFunction[Signal, Behavior[Command]] = { case akka.actor.typed.PostStop =>
    log("Client actor stopped, closing resources.")
    closeStreams(clientSocket, "ClientSocket")
    Behaviors.stopped
  }

  override def onMessage(msg: Command): Behavior[Command] =
    msg match {
      case Command.Start =>
        startReadingClient()
        this // Return the same behavior after running
    }

  private def startReadingClient(): Unit = {
    log(
      s"Client connected: ${clientSocket.getInetAddress}:${clientSocket.getPort}"
    )
    val outputStream: OutputStream = clientSocket.getOutputStream
    val ris = new RedisInputStream(clientSocket.getInputStream)

    try {

      context.log.info("Starting to read commands from client...")

      Iterator
        .continually(Protocol.read(ris))
        .takeWhile(_ != null)
        .foreach { command =>
          context.log.info(s"Received command from client: $command")
          processCommand(command, outputStream)
        }
    } finally {
      closeStreams(outputStream, "OutputStream")
      closeStreams(ris, "ClientInputStream")
    }
  }

  private def createReader(inputStream: java.io.InputStream): BufferedReader =
    new BufferedReader(
      new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)
    )

  def log(message: String): Unit = {
    // This function can be used to log messages
    println(s"LOG: $message")
  }

  def closeStreams(s: Closeable, name: String): Unit = {
    try {
      if (s != null) {
        s.close()
        log(s"$name closed successfully.")
      }
    } catch {
      case e: Exception =>
        log(s"Error closing $name: ${e.getMessage}")
    }
  }

  private def processCommand(command: ProtocolMessage, out: OutputStream): Unit = {
    // This function can be extended to handle more commands
    log(s"Processing command: $command")

    val commandHandlers: Seq[CommandDetectTrait & CommandHandler] = List(
      Ping,
      Echo
    )

    commandHandlers.find(_.canHandle(command)) match {
      case Some(handler) =>
        handler.handle(out)
      case None =>
        handleUnknownCommand(command, out)
    }
  }

  def handleUnknownCommand(command: String, out: OutputStream): Unit = {
    context.log.info(s"Unknown command: $command")
    val response = s"-ERR unknown command '$command'\r\n"
//    out.write(response.getBytes(StandardCharsets.UTF_8))
  }

}
