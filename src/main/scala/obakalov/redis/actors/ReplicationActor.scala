package obakalov.redis.actors

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}

object ReplicationActor {

  enum Command:
    case Info(replyTo: ActorRef[ClientActor.ExpectingAnswers])
  //    case StartReplication(masterHost: String, masterPort: Int)
  //    case StopReplication()
  //    case SendData()

  type ReplicationActorBehaviorType = Command

  def apply(dbActor: ActorRef[DatabaseActor.Command]): Behavior[ReplicationActorBehaviorType] =
    Behaviors.setup { context =>
      context.log.info("ReplicationActor started")
      val config = ReplicationConfig(
        role = "master", // Default role, can be changed based on actual replication setup
        slaves = Seq.empty, // Initially no slaves
        masterReplicationId = "initial-repl-id",
        masterReplicationOffset = 0L
      )
      Behaviors.receiveMessage { case Command.Info(replyTo) =>
        replyTo ! ClientActor.ExpectingAnswers.MultiBulkString(config.createReplicationInfo.toBulkString) // Placeholder for actual info response
        Behaviors.same
      }

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
    def toBulkString: Seq[Array[Byte]] =
      Seq(
        s"role:$role",
        s"connected_slaves:$connected_slaves",
        s"master_replid:$master_replid",
        s"master_repl_offset:$master_repl_offset"
      ).map(_.getBytes("UTF-8"))
  }
}
