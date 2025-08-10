package obakalov.redis.actors

import obakalov.redis.CmdArgConfig
import obakalov.redis.actors.client.ProtocolGenerator
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source, Tcp}
import org.apache.pekko.util.ByteString

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Random

object ReplicationActor {

  enum Command:
    case Info(replyTo: ActorRef[ClientActor.ExpectingAnswers])
    case InitiateHandshake
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
      Behaviors.receive {
        case (ctx, msg: Command.Info)         => handleInfo(config, msg)
        case (ctx, Command.InitiateHandshake) => handleInitiateHandshake(ctx, config)
      }
    }

  def handleInfo(
      config: ReplicationConfig,
      msg: Command.Info
  ): Behavior[ReplicationActorBehaviorType] = {
    msg.replyTo ! ClientActor.ExpectingAnswers.MultiBulkString(config.createReplicationInfo.toBulkString)
    Behaviors.same
  }

  private def handleInitiateHandshake(
      context: ActorContext[ReplicationActorBehaviorType],
      config: ReplicationConfig
  ): Behavior[ReplicationActorBehaviorType] = {

    val host: String = config.masterHost.getOrElse(throw new RuntimeException("Master host is not defined"))
    val port: Int = config.masterPort.getOrElse(6379) // Default Redis port
    val portString = port.toString

    val pingCommand = ByteString(ProtocolGenerator.generateBulkString("PONG"))
    val replconfListeningPortCommand = ByteString(ProtocolGenerator.generateBulkString("REPLCONF", "listening-port", port.toString))
    val replconfCapaPsync2Command = ByteString(ProtocolGenerator.generateBulkString("REPLCONF", "CAPA", "PSYNC2"))

    val pongResponse = ByteString(ProtocolGenerator.simpleString("PONG"))
    val okResponse = ByteString(ProtocolGenerator.simpleString("OK"))

    implicit val system: org.apache.pekko.actor.ActorSystem = context.system.classicSystem
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher

    val handshakeFuture: Future[Unit] = for {
      pong <- sendCommandToMaster(host, port, pingCommand)
      _ = if (pong != pongResponse) throw new RuntimeException(s"Unexpected PONG response from master: ${pong.utf8String}")
      ok1 <- sendCommandToMaster(host, port, replconfListeningPortCommand)
      _ = if (ok1 != okResponse) throw new RuntimeException(s"Unexpected REPLCONF listening-port response from master: ${ok1.utf8String}")
      ok2 <- sendCommandToMaster(host, port, replconfCapaPsync2Command)
      _ = if (ok2 != okResponse) throw new RuntimeException(s"Unexpected REPLCONF CAPA PSYNC2 response from master: ${ok2.utf8String}")
    } yield ()
    
    handshakeFuture.onComplete {
      case scala.util.Success(_) =>
        context.log.info("Handshake with master completed successfully")
        // Here you can send a message to the dbActor to start replication
//        dbActor ! DatabaseActor.Command.StartReplication(config)
        Behaviors.same
      case scala.util.Failure(exception) =>
        context.log.error(s"Failed to complete handshake with master: ${exception.getMessage}")
        Behaviors.stopped
    }

  }

  private def sendCommandToMaster(host: String, port: Int, command: ByteString)(implicit system: org.apache.pekko.actor.ActorSystem): Future[ByteString] = {
    Source
      .single(command)
      .via(Tcp.get(system).outgoingConnection(host, port))
      .runWith(Sink.head)
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
