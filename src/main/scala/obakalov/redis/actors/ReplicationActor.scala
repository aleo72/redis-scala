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

      Behaviors.receiveMessage { case Command.Info(replyTo) =>
        replyTo ! ClientActor.ExpectingAnswers.Ok // Placeholder for actual info response
        Behaviors.same
      }

    }

}
