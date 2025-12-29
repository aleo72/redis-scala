package obakalov.redis.actors.client.logic

import obakalov.redis.actors.client.{ExpectedResponseEnum, ProtocolMessage}
import obakalov.redis.actors.{ClientActor, DatabaseActor, ReplicationActor}
import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.util.ByteString

import java.io.OutputStream

object SetLogic extends CommandDetectTrait with CommandHandler {

  override def commandName: String = "SET"

  val validParamsPXEX = Set("PX", "EX")

  override def handle( cc: CommandContext ): ExpectedResponseEnum = {
    logCommand(cc)
    val expectedResponseEnum =
      if(cc.cmdArgConfig.isSlave)
        ExpectedResponseEnum.NoResponse
      else
        ExpectedResponseEnum.ExpectedResponse
    // Extract the key and value from the command
    cc.msg.multiBulkMessage match {
      case Some(Seq(_, keyM, valueM)) =>
        val key = keyM.bulkMessageString
        val value = valueM.bulkMessageString
        cc.log.info(s"Sending SET command to a database actor (${cc.databaseActor}) with a key: $key and value: $value, with replyTo: ${cc.replyTo}")
        cc.databaseActor ! DatabaseActor.Command.Set(key = key, value = valueM.bulkMessage, expired = None, replyTo = cc.replyTo)
        cc.replicationActor ! ReplicationActor.Command.Set(key = key, value = valueM.bulkMessage, expired = None)
        expectedResponseEnum
      case Some(Seq(_, keyM, valueM, paramM, paramValueM)) if validParamsPXEX.contains(paramM.bulkMessageString.toUpperCase) =>
        val key = keyM.bulkMessageString
        val value = valueM.bulkMessageString
        val param = paramM.bulkMessageString.toUpperCase
        val pxValueString = paramValueM.bulkMessageString

        cc.log.info(
          s"Sending SET ($param) command to database actor (${cc.databaseActor}) with key: $key, value: $value, param: $param, pxValue: $pxValueString, with replyTo: ${cc.replyTo}"
        )

        val maybePxValue: Option[Long] =
          param.toUpperCase match {
            case "PX" => paramValueM.bulkMessageLong // Expecting milliseconds
            case "EX" => paramValueM.bulkMessageLong.map(_ * 1000) // Convert seconds to milliseconds
            case _    => None
          }
        maybePxValue match {
          case pxOpt @ Some(pxValue) if pxValue >= 0 =>
            cc.databaseActor ! DatabaseActor.Command.Set(key = key, value = valueM.bulkMessage, expired = pxOpt, replyTo = cc.replyTo)
            cc.replicationActor ! ReplicationActor.Command.Set(key = key, value = valueM.bulkMessage, expired = pxOpt)
            expectedResponseEnum
          case Some(pxValue) if pxValue < 0 =>
            cc.queue.offer(ByteString("-ERR value for 'px' or 'ex' parameter must be a non-negative integer\r\n"))
            ExpectedResponseEnum.NoResponse
          case _ =>
            cc.queue.offer(ByteString("-ERR invalid value for 'px' or 'ex' parameter\r\n"))
            ExpectedResponseEnum.NoResponse
        }
      case _ =>
        cc.queue.offer(ByteString("-ERR wrong number of arguments for 'set' command\r\n"))
        ExpectedResponseEnum.NoResponse
    }
  }
}
