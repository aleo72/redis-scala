package codecrafters_redis.actors

import akka.actor.typed.{ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.scaladsl.{Flow, Sink, Source, Tcp}
import akka.stream.{Materializer, SystemMaterializer}
import akka.util.ByteString

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.{Failure, Success}

object ServerActor {

  sealed trait Command
  private final case class NewClient(connection: Tcp.IncomingConnection) extends Command

  def apply(port: Int): Behavior[Command] = Behaviors.setup { context =>
    implicit val system: ActorSystem[Nothing] = context.system
    implicit val materializer: Materializer = SystemMaterializer(context.system).materializer
    given executionContext: ExecutionContextExecutor = context.executionContext
    val connections = Tcp.get(system).bind("0.0.0.0", port)
    connections.runForeach { c =>
      context.self ! NewClient(c)
    }

    val dbActor = context.spawn(DatabaseActor(), "database-actor")

    Behaviors.receiveMessage { case NewClient(connection) =>
      val nameClient = s"client-${connection.remoteAddress.getPort}"
      context.log.info(
        s"Accepted new client connection from ${connection.remoteAddress}, spawning ClientActor with name: $nameClient"
      )
      val (queue, source) = Source
        .queue[ByteString](bufferSize = 1024, overflowStrategy = akka.stream.OverflowStrategy.dropHead)
        .preMaterialize()

      val handler = context.spawn(ClientActor(queue, dbActor), nameClient)

      val sink = Sink.foreach[ByteString] { data =>
        handler ! ClientActor.Command.ReceivedData(data)
      }

      connection.handleWith(
        Flow
          .fromSinkAndSourceCoupled(
            sink,
            source
          )
          .watchTermination() { (_, done) =>
            done.onComplete {
              case Success(_) =>
                context.log.info(s"Connection with ${connection.remoteAddress} closed.")
                handler ! ClientActor.Command.Disconnected
              case Failure(exception) =>
                context.log.error(s"Error in connection with ${connection.remoteAddress}: $exception")
                handler ! ClientActor.Command.Disconnected
            }
          }
      )
      Behaviors.same
    }
  }

  /*  sealed trait Command
  private case object AcceptNewClient extends Command

  def apply(port: Int): Behavior[Command] = Behaviors.setup { context =>
    implicit val ec: ExecutionContext = context.system.executionContext
    val serverSocket = new ServerSocket(port)

    context.log.info(s"Starting ServerActor on port $port")
    val databaseActor = context.spawn(DatabaseActor(), "database-actor")

//    context.system.receptionist ! Receptionist.Register(DatabaseActor.DatabaseKey, databaseActor)

//    context.system.receptionist ! Receptionist.Find(DatabaseActor.DatabaseKey, context.self)

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
            context.spawn(ClientActor.apply(clientSocket, databaseActor), nameClient)
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
  }*/
}
