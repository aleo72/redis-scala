package obakalov.redis.actors.client.logic

import obakalov.redis.actors.client.{CommandDetectTrait, DatabaseCommandHandler, ExpectedResponseEnum, ProtocolMessage}
import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.util.ByteString
import obakalov.redis.actors.{ClientActor, DatabaseActor}

import java.io.OutputStream

object SetLogic extends CommandDetectTrait with DatabaseCommandHandler {

  override def commandName: String = "SET"

  val validParamsPXEX = Set("PX", "EX")

  override def handle(
      command: ProtocolMessage,
      queue: SourceQueueWithComplete[ByteString],
      databaseActor: ActorRef[DatabaseActor.Command],
      replyTo: ActorRef[DatabaseActor.Response],
      log: org.slf4j.Logger
  ): ExpectedResponseEnum = {
    // Extract the key and value from the command
    command.multiBulkMessage match {
      case Some(Seq(_, keyM, valueM)) =>
        val key = keyM.bulkMessageString
        val value = valueM.bulkMessageString
        log.info(s"Sending SET command to database actor ($databaseActor) with key: $key and value: $value, with replyTo: $replyTo")
        databaseActor ! DatabaseActor.Command.Set(key = key, value = valueM.bulkMessage, expired = None, replyTo = replyTo)
        ExpectedResponseEnum.ExpectedResponse
      case Some(Seq(_, keyM, valueM, paramM, paramValueM)) if validParamsPXEX.contains(paramM.bulkMessageString.toUpperCase) =>
        val key = keyM.bulkMessageString
        val value = valueM.bulkMessageString
        val param = paramM.bulkMessageString.toUpperCase
        val pxValueString = paramValueM.bulkMessageString

        log.info(
          s"Sending SET ($param) command to database actor ($databaseActor) with key: $key, value: $value, param: $param, pxValue: $pxValueString, with replyTo: $replyTo"
        )

        val maybePxValue: Option[Long] =
          param match {
            case "PX" => paramValueM.bulkMessageLong // Expecting milliseconds
            case "EX" => paramValueM.bulkMessageLong.map(_ * 1000) // Convert seconds to milliseconds
            case _    => None
          }
        maybePxValue match {
          case pxOpt @ Some(pxValue) if pxValue >= 0 =>
            databaseActor ! DatabaseActor.Command.Set(key, valueM.bulkMessage, pxOpt, replyTo)
            ExpectedResponseEnum.ExpectedResponse
          case Some(pxValue) if pxValue < 0 =>
            queue.offer(ByteString("-ERR value for 'px' or 'ex' parameter must be a non-negative integer\r\n"))
            ExpectedResponseEnum.NoResponse
          case _ =>
            queue.offer(ByteString("-ERR invalid value for 'px' or 'ex' parameter\r\n"))
            ExpectedResponseEnum.NoResponse
        }
      case _ =>
        queue.offer(ByteString("-ERR wrong number of arguments for 'set' command\r\n"))
        ExpectedResponseEnum.NoResponse
    }
  }
}
