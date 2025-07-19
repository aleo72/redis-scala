package obakalov.redis.actors

import obakalov.redis.CmdArgConfig
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}

import scala.util.Random

object ReplicationActor {

  enum Command:
    case Info(replyTo: ActorRef[ClientActor.ExpectingAnswers])
  //    case StartReplication(masterHost: String, masterPort: Int)
  //    case StopReplication()
  //    case SendData()

  type ReplicationActorBehaviorType = Command

  def apply(cmdArgConfig: CmdArgConfig, dbActor: ActorRef[DatabaseActor.Command]): Behavior[ReplicationActorBehaviorType] =
    Behaviors.setup { context =>
      context.log.info("ReplicationActor started")
      val config = createReplicationConfig(cmdArgConfig)
      Behaviors.receiveMessage { case Command.Info(replyTo) =>
        replyTo ! ClientActor.ExpectingAnswers.MultiBulkString(config.createReplicationInfo.toBulkString) // Placeholder for actual info response
        Behaviors.same
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
      replicationBacklogHistlen = None
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
      replicationBacklogHistlen: Option[Long] = None // Optional history length of the replication backlog
  ) {
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
