package obakalov.redis.actors.client.logic

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.util.ByteString
import obakalov.redis.actors.{ClientActor, DatabaseActor}
import obakalov.redis.actors.client.{ExpectedResponseEnum, ProtocolMessage}
import org.slf4j.Logger

object KeysLogic extends CommandDetectTrait with CommandHandler {

  override def commandName: String = "KEYS"

  override def handle(cc: CommandContext): ExpectedResponseEnum =
    cc.msg.multiBulkMessage match {
      case Some(multiBulk) if multiBulk.length == 2 => // KEYS pattern
        val pattern = multiBulk(1).bulkMessageString // The pattern is the second element
        cc.log.info(s"Sending $commandName command to a database actor (${cc.databaseActor}) with a pattern: $pattern, with replyTo: ${cc.replyTo}")
        // Send the KEYS command to the database actor
        cc.databaseActor ! DatabaseActor.Command.Keys(pattern, cc.replyTo)
        ExpectedResponseEnum.ExpectedResponse
      case _ =>
        // If the command is malformed, send an error response
        cc.queue.offer(ByteString("-ERR wrong number of arguments for 'keys' command\r\n"))
        ExpectedResponseEnum.NoResponse
    }
}
