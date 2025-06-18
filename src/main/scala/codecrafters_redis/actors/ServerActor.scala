package codecrafters_redis.actors

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

import java.net.ServerSocket
import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object ServerActor {
  sealed trait Command
  private case object AcceptNewClient extends Command

  def apply(port: Int): Behavior[Command] = Behaviors.setup { context =>
    implicit val ec: ExecutionContext = context.system.executionContext
    val serverSocket = new ServerSocket(port)

    context.log.info(s"Starting ServerActor on port $port")

    def handleCommand(command: Command): Behavior[Command] = command match {
      case AcceptNewClient =>
        context.log.info("Waiting for new client connections...")
        context.pipeToSelf(scala.concurrent.Future(serverSocket.accept())) {
          case Success(clientSocket) =>
            context.log.info(
              s"Accepted new client connection from ${clientSocket.getInetAddress}"
            )
            val nameClient = s"client-${UUID.randomUUID()}"
            context.log.info(s"Spawning ClientActor with name: $nameClient")
            context.spawn(ClientActor.apply(clientSocket), nameClient)
            AcceptNewClient
          case Failure(exception) =>
            context.log.error(s"Failed to accept client connection: $exception")
            AcceptNewClient
        }
        Behaviors.same
    }

    context.self ! AcceptNewClient

    Behaviors
      .receiveMessage(handleCommand)
      .receiveSignal { case (_, akka.actor.typed.PostStop) =>
        context.log.info("ServerActor stopped, closing server socket.")
        serverSocket.close()
        Behaviors.stopped
      }
  }
}
