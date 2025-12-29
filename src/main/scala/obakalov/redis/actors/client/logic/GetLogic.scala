package obakalov.redis.actors.client.logic


import obakalov.redis.actors.DatabaseActor
import obakalov.redis.actors.client.logic.{CommandContext, CommandDetectTrait, CommandHandler}
import obakalov.redis.actors.client.ExpectedResponseEnum

import org.apache.pekko.util.ByteString
import org.slf4j.Logger

object GetLogic extends CommandDetectTrait with CommandHandler {

  override def commandName: String = "GET"

  override def handle(cc: CommandContext): ExpectedResponseEnum =
    cc.msg.multiBulkMessage match {
      case Some(multiBulk) if multiBulk.length == 2 =>
        val key = multiBulk(1).bulkMessageString
        cc.log.info(s"Sending GET command to a database actor (${cc.databaseActor}) with key: $key, with replyTo: ${cc.replyTo}")
        // Send the GET command to the database actor
        cc.databaseActor ! DatabaseActor.Command.Get(key, cc.replyTo)
        ExpectedResponseEnum.ExpectedResponse
      case _ =>
        // If the command is malformed, send an error response
        cc.queue.offer(ByteString("-ERR wrong number of arguments for 'get' command\r\n"))
        ExpectedResponseEnum.NoResponse
    }
}
