package obakalov.redis.actors.client.logic

import obakalov.redis.actors.client.{ExpectedResponseEnum, ProtocolMessage}
import obakalov.redis.actors.{ClientActor, DatabaseActor}
import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.util.ByteString
import org.slf4j.Logger

object ConfigLogic extends CommandDetectTrait with CommandHandler {

  override def commandName: String = "CONFIG"

  override def handle(cc: CommandContext): ExpectedResponseEnum =
    cc.msg.multiBulkMessage match {
      case Some(multiBulk) if multiBulk.length == 3 && multiBulk(1).bulkMessageString.equalsIgnoreCase("GET") =>
        val action = multiBulk(1).bulkMessageString
        val key = multiBulk(2).bulkMessageString
        cc.log.info(s"Sending $commandName command to a database actor (${cc.databaseActor}) with action: $action, key: $key, with replyTo: ${cc.replyTo}")

        cc.databaseActor ! DatabaseActor.Command.Config(Option(key), None, cc.replyTo)
        ExpectedResponseEnum.ExpectedResponse
      case _ =>
        // If the command is malformed, send an error response
        cc.queue.offer(ByteString("-ERR wrong number of arguments for 'get' command\r\n"))
        ExpectedResponseEnum.NoResponse
    }
}
