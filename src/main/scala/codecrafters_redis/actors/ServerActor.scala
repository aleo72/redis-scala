package codecrafters_redis.actors

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}

import java.net.ServerSocket
import java.util.UUID
import scala.concurrent.ExecutionContext

object ServerActor {
  sealed trait Command
  private case object AcceptNewClient extends Command

  def props(port: Int): Behavior[ServerActor.Command] = Behaviors.setup { ctx =>
    new ServerActor(ctx, port)
  }
}

class ServerActor(context: ActorContext[ServerActor.Command], port: Int)
    extends AbstractBehavior[ServerActor.Command](context) {
  import ServerActor._

  private val serverSocket = new ServerSocket(port)
  private implicit val ec: ExecutionContext = context.system.executionContext

  context.self.tell(ServerActor.AcceptNewClient)

  override def onMessage(msg: Command): Behavior[Command] = msg match {
    case AcceptNewClient =>
      context.pipeToSelf(scala.concurrent.Future(serverSocket.accept())) {
        case scala.util.Success(clientSocket) =>
          context.log.info(
            s"Accepted new client connection from ${clientSocket.getInetAddress}"
          )
          val nameClient = s"client-${UUID.randomUUID()}"
          context.log.info(s"Spawning ClientActor with name: $nameClient")
          context.spawn(ClientActor.props(clientSocket), nameClient)
          AcceptNewClient
        case scala.util.Failure(exception) =>
          context.log.error(s"Failed to accept client connection: $exception")
          AcceptNewClient
      }
      this
  }

  override def onSignal
      : PartialFunction[akka.actor.typed.Signal, Behavior[Command]] = {
    case akka.actor.typed.PostStop =>
      context.log.info("ServerActor stopped, closing server socket.")
      serverSocket.close()
      Behaviors.stopped
  }

}
