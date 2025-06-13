package codecrafters_redis.actors

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{Behavior, Signal}

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

  override def onSignal: PartialFunction[Signal, Behavior[Command]] = {
    case akka.actor.typed.PostStop =>
      log("Client actor stopped, closing resources.")
      closeStreams(clientSocket, "ClientSocket")
      Behaviors.stopped
  }

  override def onMessage(msg: Command): Behavior[Command] =
    msg match {
      case Command.Start =>
        run()
        this // Return the same behavior after running
    }

  def run(): Unit = {
    log(
      s"Client connected: ${clientSocket.getInetAddress}:${clientSocket.getPort}"
    )
    val outputStream: OutputStream = clientSocket.getOutputStream
    val reader = createReader(clientSocket.getInputStream)

    try {
      while (true) {
        val command = reader.readLine()
        if (command == null) {
          log("Client disconnected")
          return
        }
        log(s"Received command from client: $command")
        processCommand(command, outputStream)
      }
    } finally {
      closeStreams(outputStream, "OutputStream")
      closeStreams(reader, "ClientInputStream")
    }
  }

  def createReader(inputStream: java.io.InputStream): BufferedReader =
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

  private def processCommand(command: String, out: OutputStream): Unit = {
    // This function can be extended to handle more commands
    log(s"Processing command: $command")
    command match {
      case "PING" => out.write("+PONG\r\n".getBytes(StandardCharsets.UTF_8));
      case _ =>
        log(s"Unknown command: $command")
      //        "-ERR unknown command\r\n"
    }
  }
}
