package codecrafters_redis.actors

import org.apache.pekko.actor.typed.{ActorSystem, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source, Tcp}
import org.apache.pekko.stream.{Materializer, SystemMaterializer}
import org.apache.pekko.util.ByteString
import codecrafters_redis.CmdArgConfig

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.{Failure, Success}

object ServerActor {

  sealed trait Command
  private final case class NewClient(connection: Tcp.IncomingConnection) extends Command

  def apply(cmdArgConfig: CmdArgConfig): Behavior[Command] = Behaviors.setup { context =>
    implicit val system: ActorSystem[Nothing] = context.system
    implicit val materializer: Materializer = SystemMaterializer(context.system).materializer
    given executionContext: ExecutionContextExecutor = context.executionContext
    val connections = Tcp.get(system).bind("0.0.0.0", cmdArgConfig.port)
    connections.runForeach { c =>
      context.self ! NewClient(c)
    }

    val dbActor = context.spawn(DatabaseActor(cmdArgConfig), "database-actor")

    Behaviors.receiveMessage { case NewClient(connection) =>
      val nameClient = s"client-${connection.remoteAddress.getPort}"
      context.log.info(
        s"Accepted new client connection from ${connection.remoteAddress}, spawning ClientActor with name: $nameClient"
      )
      val (queue, source) = Source
        .queue[ByteString](bufferSize = 1024, overflowStrategy = org.apache.pekko.stream.OverflowStrategy.dropHead)
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
}
