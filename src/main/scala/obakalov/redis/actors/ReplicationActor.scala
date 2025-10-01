package obakalov.redis.actors

import obakalov.redis.CmdArgConfig
import obakalov.redis.actors.ReplicationActor.Command.WrappedConnectionResponse
import obakalov.redis.actors.client.ProtocolGenerator
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.stream.scaladsl.{Sink, Source, Tcp}
import org.apache.pekko.util.ByteString

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Random

object ReplicationActor {

  type ContextType = ActorContext[ReplicationActorBehaviorType]

  enum Command:
    case Info(replyTo: ActorRef[ClientActor.ExpectingAnswers])
    case InitiateHandshake

    case WrappedConnectionResponse(response: PersistentConnectionActor.Response)
  //    case StartReplication(masterHost: String, masterPort: Int)
  //    case StopReplication()
  //    case SendData()

  type ReplicationActorBehaviorType = Command

  def apply(cmdArgConfig: CmdArgConfig, dbActor: ActorRef[DatabaseActor.Command]): Behavior[ReplicationActorBehaviorType] =
    Behaviors.setup { context =>
      context.log.info("ReplicationActor started")
      val config: ReplicationConfig = createReplicationConfig(cmdArgConfig)
      if (config.isSlave) {
        context.self ! Command.InitiateHandshake
      }

      val responseAdapter: ActorRef[PersistentConnectionActor.Response] = context.messageAdapter(Command.WrappedConnectionResponse.apply)
      val host: String = config.masterHost.getOrElse(throw new RuntimeException("Master host is not defined"))
      val port: Int = config.masterPort.getOrElse(6379) // Default Redis port

      val connectionActor: ActorRef[PersistentConnectionActor.Command] =
        context.spawn(PersistentConnectionActor(host, port), s"PersistentConnectionActor-$host-$port")


      Behaviors.receive {
        case (ctx, msg: Command.Info)         => handleInfo(config, msg, connectionActor)
        case (ctx, Command.InitiateHandshake) => handleInitiateHandshake(ctx, config, connectionActor, responseAdapter)
      }
    }

  def handleInfo(
      config: ReplicationConfig,
      msg: Command.Info,
      connectionActor: ActorRef[PersistentConnectionActor.Command]
  ): Behavior[ReplicationActorBehaviorType] = {
    msg.replyTo ! ClientActor.ExpectingAnswers.MultiBulkString(config.createReplicationInfo.toBulkString)
    Behaviors.same
  }

  def replicationActive(
      config: ReplicationConfig,
      connectionActor: ActorRef[PersistentConnectionActor.Command],
      responseAdapter: ActorRef[PersistentConnectionActor.Response]
  ): Behavior[ReplicationActorBehaviorType] = Behaviors.setup { context =>
    Behaviors.receiveMessage { m =>
      context.log.info(s"Replication active, received message: $m")
      Behaviors.same
    }
  }

  private def handleInitiateHandshake(
      context: ContextType,
      config: ReplicationConfig,
      connectionActor: ActorRef[PersistentConnectionActor.Command],
      responseAdapter: ActorRef[PersistentConnectionActor.Response]
  ): Behavior[ReplicationActorBehaviorType] = {
    context.log.info(s"Initiating handshake with master at ${config.masterHost.get}:${config.masterPort.get}")
    context.log.info(s"Sending PING command to master")
    connectionActor ! PersistentConnectionActor.Send(ByteString(ProtocolGenerator.generateBulkString("PING")), responseAdapter)
    context.log.info(s"Sent PING command to master, awaiting PONG response")
    awaitingHandshakePong(config, connectionActor, responseAdapter)
  }

  def awaitingHandshakePong(
      config: ReplicationConfig,
      connectionActor: ActorRef[PersistentConnectionActor.Command],
      responseAdapter: ActorRef[PersistentConnectionActor.Response]
  ): Behavior[ReplicationActorBehaviorType] =
    Behaviors.setup { context =>
      Behaviors.receiveMessage {
        case Command.WrappedConnectionResponse(response) =>
          response match {
            case PersistentConnectionActor.Response.ConnectionFailure(reason) =>
              context.log.error(s"Connection failure during handshake: $reason")
              Behaviors.stopped
            case PersistentConnectionActor.Response.ServerResponse(data) if data.utf8String.startsWith("+PONG") =>
              val portString = config.masterPort.getOrElse(6379).toString
              context.log.info(s"Received PONG from master, sending REPLCONF listening-port $portString command")
              val portMessageCommand = ByteString(ProtocolGenerator.generateBulkString("REPLCONF", "listening-port", portString))
              connectionActor ! PersistentConnectionActor.Send(portMessageCommand, responseAdapter)
              awaitingReplconfListeningPort(config, connectionActor, responseAdapter)
            case PersistentConnectionActor.Response.ServerResponse(data) =>
              context.log.error(s"Unexpected response during handshake: ${data.utf8String.trim}, expecting +PONG")
              Behaviors.stopped
          }
        case m =>
          context.log.warn(s"Unexpected message during handshake: $m")
          Behaviors.unhandled
      }
    }

  def awaitingReplconfListeningPort(
      config: ReplicationConfig,
      connectionActor: ActorRef[PersistentConnectionActor.Command],
      responseAdapter: ActorRef[PersistentConnectionActor.Response]
  ): Behavior[ReplicationActorBehaviorType] =
    Behaviors.setup { context =>
      Behaviors.receiveMessage {
        case Command.WrappedConnectionResponse(response) =>
          response match {
            case PersistentConnectionActor.Response.ConnectionFailure(reason) =>
              context.log.error(s"Connection failure during REPLCONF listening-port: $reason")
              Behaviors.stopped
            case PersistentConnectionActor.Response.ServerResponse(data) if data.utf8String.startsWith("+OK") =>
              context.log.info(s"Received +OK from master for REPLCONF listening-port, sending REPLCONF CAPA PSYNC2 command")
              val capaMessageCommand = ByteString(ProtocolGenerator.generateBulkString("REPLCONF", "CAPA", "PSYNC2"))
              connectionActor ! PersistentConnectionActor.Send(capaMessageCommand, responseAdapter)
              awaitingReplconfCapa(config, connectionActor, responseAdapter)
            case PersistentConnectionActor.Response.ServerResponse(data) =>
              context.log.error(s"Unexpected response during REPLCONF listening-port: ${data.utf8String.trim}, expecting +OK")
              Behaviors.stopped
          }
        case m =>
          context.log.warn(s"Unexpected message during REPLCONF listening-port: $m")
          Behaviors.unhandled
      }
    }
  def awaitingReplconfCapa(
      config: ReplicationConfig,
      connectionActor: ActorRef[PersistentConnectionActor.Command],
      responseAdapter: ActorRef[PersistentConnectionActor.Response]
  ): Behavior[ReplicationActorBehaviorType] =
    Behaviors.setup { context =>
      Behaviors.receiveMessage {
        case Command.WrappedConnectionResponse(response) =>
          response match {
            case PersistentConnectionActor.Response.ConnectionFailure(reason) =>
              context.log.error(s"Connection failure during REPLCONF CAPA: $reason")
              Behaviors.stopped
            case PersistentConnectionActor.Response.ServerResponse(data) if data.utf8String.startsWith("+OK") =>
              context.log.info(s"Received +OK from master for REPLCONF CAPA, sending PSYNC command")
              replicationActive(config, connectionActor, responseAdapter)
            case PersistentConnectionActor.Response.ServerResponse(data) =>
              context.log.error(s"Unexpected response during REPLCONF CAPA: ${data.utf8String.trim}, expecting +OK")
              Behaviors.stopped
          }
        case m =>
          context.log.warn(s"Unexpected message during REPLCONF CAPA: $m")
          Behaviors.unhandled
      }
    }

  private def sendCommandToMaster(host: String, port: Int, command: ByteString)(implicit context: ContextType): Future[ByteString] = {
    implicit val system: ActorSystem = context.system.classicSystem
    implicit val executionContext: ExecutionContextExecutor = context.system.executionContext
    println(s"Connecting to master at $host:$port: sending command ${command.utf8String.trim}")
    Source
      .single(command)
      .via(Tcp().outgoingConnection(host, port))
      .runWith(Sink.headOption)
      .map { response =>
        println(s"Received response from master: ${response.map(_.utf8String.trim)}")
        response.getOrElse(
          throw new RuntimeException(s"Failed to receive response from master at $host:$port")
        )
      }

  }

  private def createReplicationConfig(
      config: CmdArgConfig
  ): ReplicationConfig = {
    ReplicationConfig(
      role = if (config.replicaof.isEmpty) "master" else "slave",
      slaves = Seq.empty, // Initially no slaves
      masterReplicationId = Random.alphanumeric.take(40).mkString, // Generate a random replication ID
      masterReplicationOffset = 0,
      secondaryReplicationOffset = None,
      replicationBacklogActive = 0,
      replicationBacklogSize = 0,
      replicationBacklogFirstByte = None,
      replicationBacklogHistlen = None,
      masterHost = config.replicaof.map(_.split(" ").head),
      masterPort = config.replicaof.flatMap(_.split(" ").lastOption).map(_.toInt),
      selfPort = Option(config.port) // Use the configured port for the slave
    )
  }

  case class ReplicationConfig(
      role: String, // "master" or "slave"
      slaves: Seq[String], // List of slave addresses
      masterReplicationId: String, // Master replication ID
      masterReplicationOffset: Long, // Master replication offset
      secondaryReplicationOffset: Option[Long] = None, // Optional secondary replication offset for slaves
      replicationBacklogActive: Int = 0, // Active replication backlog size
      replicationBacklogSize: Int = 0, // Total replication backlog size
      replicationBacklogFirstByte: Option[Long] = None, // Optional first byte of the replication backlog
      replicationBacklogHistlen: Option[Long] = None, // Optional history length of the replication backlog
      masterHost: Option[String] = None, // Optional master host for slave
      masterPort: Option[Int] = None, // Optional master port for slave
      selfPort: Option[Int] = None // Optional self port for slave (if applicable)
  ) {

    def isMaster: Boolean = role == "master"
    def isSlave: Boolean = role == "slave"

    def createReplicationInfo: ReplicationInfo = ReplicationInfo(
      role = role,
      connected_slaves = slaves.size,
      master_replid = masterReplicationId,
      master_repl_offset = masterReplicationOffset
    )
  }
  case class ReplicationInfo(
      role: String, // "master" or "slave"
      connected_slaves: Int,
      master_replid: String, // Master replication ID
      master_repl_offset: Long // Master replication offset
  ) {
    def toBulkString: Seq[Array[Byte]] = Seq(
      Seq(
        s"master_replid:$master_replid",
        s"master_repl_offset:$master_repl_offset",
        s"role:$role",
        s"connected_slaves:$connected_slaves"
      ).mkString("\n")
    ).map(_.getBytes("UTF-8"))
  }
}
