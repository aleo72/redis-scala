package obakalov.redis.actors

import obakalov.redis.actors.PersistentConnectionActor.InternalCommand.{FromServer, StreamFailure}
import org.apache.pekko.Done
import org.apache.pekko.actor.typed.*
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.stream.BoundedSourceQueue
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source, Tcp}
import org.apache.pekko.util.ByteString

import scala.concurrent.Future
import scala.util.{Failure, Success}

object PersistentConnectionActor {

  sealed trait Command
  final case class Send(data: ByteString, replyTo: ActorRef[Response]) extends Command

  enum InternalCommand extends Command {
    case StreamInitialized(queue: BoundedSourceQueue[ByteString])
    case FromServer(data: ByteString)
    case StreamFailure(exception: Throwable)
    case StreamCompleted
  }

  enum Response {
    case ServerResponse(data: ByteString)
    case ConnectionFailure(reason: String)
  }

  def apply(host: String, port: Int): Behavior[Command] = {
    Behaviors.setup { context =>
      implicit val system: ActorSystem[Nothing] = context.system
      val source: Source[ByteString, BoundedSourceQueue[ByteString]] = Source.queue[ByteString](bufferSize = 1000)

      val sink: Sink[ByteString, Future[Done]] = Sink.foreach(bytes => context.self.tell(FromServer(bytes)))

      val tcpFlow: Flow[ByteString, ByteString, Future[Tcp.OutgoingConnection]] = Tcp(system).outgoingConnection(host, port)

      val (queue, streamDoneFuture) = source.via(tcpFlow).toMat(sink)(Keep.both).run()

      context.pipeToSelf(Future.successful(queue)) {
        case Success(value)     => InternalCommand.StreamInitialized(value)
        case Failure(exception) => InternalCommand.StreamFailure(exception)
      }
      context.pipeToSelf(streamDoneFuture) {
        case Success(_)         => InternalCommand.StreamCompleted
        case Failure(exception) => InternalCommand.StreamFailure(exception)
      }

      initializing()
    }
  }

  private def initializing(): Behavior[Command] = Behaviors.receiveMessage {
    case InternalCommand.StreamInitialized(queue) =>
      Behaviors.setup { context =>
        context.log.info("Stream initialized, switching to active behavior")
        active(queue)
      }
    case InternalCommand.StreamFailure(ex) =>
      Behaviors.setup { context =>
        context.log.error("Stream failed during initialization", ex)
        Behaviors.stopped
      }
    case _ => Behaviors.unhandled
  }

  private def active(queue: BoundedSourceQueue[ByteString]): Behavior[Command] = Behaviors.receive {
    case (context, Send(data, replyTo)) =>
      context.log.debug(s"Sending data: ${data.utf8String.trim}")
      queue.offer(data)
      awaitingResponse(queue, replyTo)
    case (context, FromServer(data)) =>
      context.log.info(s"Received data from server: ${data.utf8String.trim}")
      Behaviors.same
    case (context, InternalCommand.StreamFailure(ex)) =>
      context.log.error("Stream failed", ex)
      Behaviors.stopped
    case (context, InternalCommand.StreamCompleted) =>
      context.log.info("Stream completed")
      Behaviors.stopped
    case _ => Behaviors.unhandled
  }

  private def awaitingResponse(queue: BoundedSourceQueue[ByteString], requester: ActorRef[Response]): Behavior[Command] = Behaviors.receiveMessagePartial {
    case FromServer(data) =>
      requester ! Response.ServerResponse(data)
      active(queue)
    case Send(_, replyTo) =>
      Behaviors.setup { context =>
        context.log.warn("Already awaiting response, cannot send new data")
        Behaviors.same
      }
    case InternalCommand.StreamFailure(ex) =>
      requester ! Response.ConnectionFailure(ex.getMessage)
      Behaviors.stopped

  }

}
