package obakalov.redis.actors

import obakalov.redis.CmdArgConfig
import obakalov.redis.actors.ReplicationActor.Command.WrappedConnectionResponse
import obakalov.redis.actors.client.ProtocolGenerator
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.util.ByteString

import scala.util.Random

object ReplicationActor {

  type ContextType = ActorContext[ReplicationActorBehaviorType]

  enum Command:
    case Info(replyTo: ActorRef[ClientActor.ComandOrResponse])
    case ReplConf(replyTo: ActorRef[ClientActor.ComandOrResponse], params: Map[String, String])
    case Psync(replyTo: ActorRef[ClientActor.ComandOrResponse], runId: String, offset: Long)
    case InitiateHandshake
    case WrappedConnectionResponse(response: PersistentConnectionActor.Response)
    case Set(key: String, value: Option[Array[Byte]], expired: Option[Long])

  enum State:
    case Idle
    case HandshakingInit
    case HandshakingPingAwaitPong
    case HandshakingReplconfListeningPortAwaitOk
    case HandshakingReplconfCapaAwaitOk
    case HandshakingPsyncAwaitResponse

  type ReplicationActorBehaviorType = Command

  def apply(cmdArgConfig: CmdArgConfig, dbActor: ActorRef[DatabaseActor.Command]): Behavior[ReplicationActorBehaviorType] =
    Behaviors.setup { context =>
      context.log.info("ReplicationActor started")
      val config: ReplicationConfig = createReplicationConfig(cmdArgConfig)
      if (config.isSlave) {
        slaveLogic(config, dbActor)
      } else {
        masterLogic(config, dbActor)
      }
    }

  private def masterLogic(config: ReplicationConfig, dbActor: ActorRef[DatabaseActor.Command]): Behavior[ReplicationActorBehaviorType] = Behaviors.setup {
    context =>
      context.log.info(s"Starting master replication logic: config: ${config}")
      // Master-specific replication logic would go here
      Behaviors.receiveMessage {
        case msg: Command.Info     => handleInfo(config, msg)
        case cmd: Command.ReplConf => handleReplConf(config, cmd, dbActor)
        case psync: Command.Psync  => handlePsync(config, psync, dbActor)
        case set: Command.Set      => masterLogicHandleSet(config, set)
        case msg                   =>
          context.log.warn(s"Master received unexpected message: $msg")
          Behaviors.unhandled
      }
  }

  private def slaveLogic(config: ReplicationConfig, dbActor: ActorRef[DatabaseActor.Command]): Behavior[ReplicationActorBehaviorType] = Behaviors.setup {
    context =>
      context.log.info("Starting slave login process")

      val host: String = config.masterHost.getOrElse(throw new RuntimeException("Master host is not defined"))
      val port: Int = config.masterPort.getOrElse(6379) // Default Redis port
      context.log.info(s"Connecting to master at $host:$port")
      val responseAdapter: ActorRef[PersistentConnectionActor.Response] = context.messageAdapter(Command.WrappedConnectionResponse.apply)
      val connectionActor: ActorRef[PersistentConnectionActor.Command] =
        context.spawn(PersistentConnectionActor(host, port), s"PersistentConnectionActor-$host-$port")

      context.self ! Command.InitiateHandshake
      handleInitiateHandshake(config, connectionActor, responseAdapter, State.HandshakingInit)
  }

  private def masterLogicHandleSet(config: ReplicationConfig, msg: Command.Set): Behavior[ReplicationActorBehaviorType] =
    Behaviors.setup { context =>
      context.log.info(s"Master received SET command for key: ${msg.key}")

      val cmdArgs = Seq(
        ByteString("SET"),
        ByteString(msg.key),
        ByteString(msg.value.getOrElse(Array.emptyByteArray)),
        msg.expired.map(_ => ByteString("PX")).getOrElse(ByteString.empty),
        msg.expired.map(px => ByteString(px.toString)).getOrElse(ByteString.empty)
      ).filter(_.nonEmpty)

      val encodedCommand = ProtocolGenerator.generateRespArray(cmdArgs*)

      config.replicaActors.foreach { replica =>
        context.log.info(s"Propagating SET command to replica: $replica")
        replica ! ClientActor.Command.SendToClient(encodedCommand)
      }

      Behaviors.same[ReplicationActorBehaviorType]
    }

  def handleInfo(
      config: ReplicationConfig,
      msg: Command.Info
  ): Behavior[ReplicationActorBehaviorType] =
    Behaviors.setup { context =>
      context.log.info("Received INFO command")
      msg.replyTo ! ClientActor.ExpectingAnswers.MultiBulkString(config.createReplicationInfo.toBulkString)
      Behaviors.same[ReplicationActorBehaviorType]
    }

  def handleReplConf(
      config: ReplicationConfig,
      msg: Command.ReplConf,
      dbActor: ActorRef[DatabaseActor.Command]
  ): Behavior[ReplicationActorBehaviorType] =
    Behaviors.setup { context =>
      context.log.info(s"Received REPLCONF command with params: ${msg.params}")

      val newConfig = msg.params.foldLeft(config) { case (config, (conf, param)) =>
        conf.toLowerCase match {
          case "listening-port" =>
            config.copy(slaves = (config.slaves :+ s"${msg.params.getOrElse("ip", "unknown")}:$param").distinct)
          case "capa" if param.toUpperCase == "PSYNC2" =>
            config // Acknowledge PSYNC2 capability
          case _ =>
            config // Ignore unknown parameters for now
        }
      }

      msg.replyTo ! ClientActor.ExpectingAnswers.Ok

      masterLogic(newConfig, dbActor)
    }

  def handlePsync(config: ReplicationConfig, msg: Command.Psync, dbActor: ActorRef[DatabaseActor.Command]): Behavior[ReplicationActorBehaviorType] =
    Behaviors.setup { context =>
      context.log.info(s"Received PSYNC command with runId: ${msg.runId},offset: ${msg.offset}")
      // For simplicity, always respond with FULLRESYNC
      val newRunId = Random.alphanumeric.take(40).mkString
      msg.replyTo ! ClientActor.ExpectingAnswers.SimpleString(s"FULLRESYNC $newRunId 0")

      // empty database response
      val rdbBytes = java.util.Base64.getDecoder.decode("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")
      msg.replyTo ! ClientActor.ExpectingAnswers.DirectValue(rdbBytes)
      
      masterLogic(config.copy(replicaActors = (config.replicaActors :+ msg.replyTo).distinct), dbActor)
    }

  def replicationActive(
      config: ReplicationConfig,
      connectionActor: ActorRef[PersistentConnectionActor.Command],
      responseAdapter: ActorRef[PersistentConnectionActor.Response]
  ): Behavior[ReplicationActorBehaviorType] = Behaviors.setup { context =>
    Behaviors.receiveMessage {
      case msg: Command.Info =>
        context.log.info("Received INFO command")
        handleInfo(config, msg)
      case m =>
        context.log.info(s"Replication active, received message: $m")
        Behaviors.same
    }
  }

  private def handleInitiateHandshake(
      config: ReplicationConfig,
      connectionActor: ActorRef[PersistentConnectionActor.Command],
      responseAdapter: ActorRef[PersistentConnectionActor.Response],
      state: State
  ): Behavior[ReplicationActorBehaviorType] = Behaviors.setup { context =>
    Behaviors.receiveMessage {
      case msg: Command.Info         => handleInfo(config, msg)
      case Command.InitiateHandshake =>
        context.log.info(s"Initiating handshake with master at ${config.masterHost.get}:${config.masterPort.get}")
        context.log.info(s"Sending PING command to master")
        connectionActor ! PersistentConnectionActor.Send(ByteString(ProtocolGenerator.generateBulkString("PING")), responseAdapter)
        context.log.info(s"Sent PING command to master, awaiting PONG response")
        handleInitiateHandshake(config, connectionActor, responseAdapter, State.HandshakingPingAwaitPong)
      case WrappedConnectionResponse(response) =>
        response match {
          case PersistentConnectionActor.Response.ConnectionFailure(reason) =>
            context.log.error(s"Connection failure during handshake: $reason")
            Behaviors.stopped
          case PersistentConnectionActor.Response.ServerResponse(data) if data.utf8String.startsWith("+PONG") && state == State.HandshakingPingAwaitPong =>
            val portString = config.currentPort.toString
            context.log.info(s"Received PONG from master, sending REPLCONF listening-port $portString command")
            val portMessageCommand = ByteString(ProtocolGenerator.generateBulkString("REPLCONF", "listening-port", portString))
            connectionActor ! PersistentConnectionActor.Send(portMessageCommand, responseAdapter)
            handleInitiateHandshake(config, connectionActor, responseAdapter, State.HandshakingReplconfListeningPortAwaitOk)
          case PersistentConnectionActor.Response.ServerResponse(data)
              if data.utf8String.startsWith("+OK") && state == State.HandshakingReplconfListeningPortAwaitOk =>
            context.log.info(s"Received +OK from master for REPLCONF listening-port, sending REPLCONF CAPA PSYNC2 command")
            val capaMessageCommand = ByteString(ProtocolGenerator.generateBulkString("REPLCONF", "CAPA", "PSYNC2"))
            connectionActor ! PersistentConnectionActor.Send(capaMessageCommand, responseAdapter)
            handleInitiateHandshake(config, connectionActor, responseAdapter, State.HandshakingReplconfCapaAwaitOk)
          case PersistentConnectionActor.Response.ServerResponse(data) if data.utf8String.startsWith("+OK") && state == State.HandshakingReplconfCapaAwaitOk =>
            context.log.info(s"Received +OK from master for REPLCONF CAPA, sending PSYNC command")
            val psyncCommand = ByteString(ProtocolGenerator.generateBulkString("PSYNC", "?", "-1"))
            connectionActor ! PersistentConnectionActor.Send(psyncCommand, responseAdapter)
            handleInitiateHandshake(config, connectionActor, responseAdapter, State.HandshakingPsyncAwaitResponse)
          case PersistentConnectionActor.Response.ServerResponse(data)
              if data.utf8String.startsWith("+FULLRESYNC") && state == State.HandshakingPsyncAwaitResponse =>
            context.log.info(s"Received FULLRESYNC from master, replication established")
            replicationActive(config, connectionActor, responseAdapter)
          case PersistentConnectionActor.Response.ServerResponse(data) =>
            context.log.error(s"Unexpected response during handshake: ${data.utf8String.trim}")
            Behaviors.stopped
        }
      case msg: ReplicationActorBehaviorType =>
        context.log.warn(s"Unexpected message during handshake initiation: $msg")
        Behaviors.unhandled
    }
  }

  private def createReplicationConfig(
      config: CmdArgConfig
  ): ReplicationConfig = {
    ReplicationConfig(
      currentPort = config.port,
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
      currentPort: Int = 6379,
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
      selfPort: Option[Int] = None, // Optional self port for slave (if applicable)
      replicaActors: Seq[ActorRef[ClientActor.ComandOrResponse]] = Seq.empty
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
