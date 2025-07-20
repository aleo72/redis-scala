package obakalov.redis.actors

import obakalov.redis.CmdArgConfig
import obakalov.redis.actors.ReplicationActor.Command
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.stream.scaladsl.{Flow, Source, Tcp, Sink}
import org.apache.pekko.util.ByteString

import scala.concurrent.Future
import scala.util.Random

object ReplicationActor {

  enum Command:
    case Info(replyTo: ActorRef[ClientActor.ExpectingAnswers])
    case SendPingToMaster()
  //    case StartReplication(masterHost: String, masterPort: Int)
  //    case StopReplication()
  //    case SendData()

  type ReplicationActorBehaviorType = Command

  def apply(cmdArgConfig: CmdArgConfig, dbActor: ActorRef[DatabaseActor.Command]): Behavior[ReplicationActorBehaviorType] =
    Behaviors.setup { context =>
      context.log.info("ReplicationActor started")
      val config: ReplicationConfig = createReplicationConfig(cmdArgConfig)
      if (config.isSlave) {
        context.self ! Command.SendPingToMaster()
      }
      Behaviors.receive {

        case (ctx, msg: Command.Info)             => handleInfo(config, msg)
        case (ctx, msg: Command.SendPingToMaster) => handleSendPingToMaster(config, msg)
      }
    }
  private def handleSendPingToMaster(
      config: ReplicationConfig,
      msg: Command.SendPingToMaster
  ): Behavior[ReplicationActorBehaviorType] = {
    val pingCommand = ByteString("*1\\r\\n$4\\r\\nPING\\r\\n")
    val host = config.masterHost.getOrElse(new RuntimeException("Master host is not defined"))
    val port = config.masterPort.getOrElse(6379) // Default Redis port

    implicit val system = context.system

    val source: Source[ByteString, _] = Source.single(pingCommand)
    val connectionFlow: Flow[ByteString, ByteString, Future[Tcp.OutgoingConnection]] = Tcp().outgoingConnection(host, port)
    val pingStream: Source[ByteString, Future[Tcp.OutgoingConnection]] = source.viaMat(connectionFlow)((_, connectionF) => connectionF)
    val connectionFuture = pingStream.runWith(Sink.ignore)

    Behaviors.same

  }

  def handleInfo(
      config: ReplicationConfig,
      msg: Command.Info
  ): ClientActor.ExpectingAnswers = {
    msg.replyTo ! ClientActor.ExpectingAnswers.MultiBulkString(replicationInfo.toBulkString)
    Behaviors.same
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
      masterPort = config.replicaof.flatMap(_.split(" ").lastOption).map(_.toInt)
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
      masterPort: Option[Int] = None // Optional master port for slave
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
