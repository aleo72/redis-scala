package codecrafters_redis.commands.logic

import akka.actor.typed.ActorRef
import akka.stream.scaladsl.SourceQueueWithComplete
import akka.util.ByteString
import codecrafters_redis.actors.{ClientActor, DatabaseActor}
import codecrafters_redis.commands.{CommandDetectTrait, CommandHandler, ExpectedResponse, ProtocolMessage}

import java.io.OutputStream

object SetLogic extends CommandDetectTrait with CommandHandler {

  override def commandName: String = "SET"

  val validParamsPXEX = Set("PX", "EX")

  override def handle(
      command: ProtocolMessage,
      queue: SourceQueueWithComplete[ByteString],
      databaseActor: ActorRef[DatabaseActor.Command],
      replyTo: ActorRef[DatabaseActor.Response],
      log: org.slf4j.Logger
  ): ExpectedResponse = {
    // Extract the key and value from the command
    command.multiBulkMessage match {
      case Some(Seq(_, keyM, valueM)) =>
        val key = keyM.bulkMessageString
        val value = valueM.bulkMessageString
        log.info(s"Sending SET command to database actor ($databaseActor) with key: $key and value: $value, with replyTo: $replyTo")
        databaseActor ! DatabaseActor.Command.Set(key = key, value = valueM.bulkMessage, expired = None, replyTo = replyTo)
        ExpectedResponse.ExpectedResponse
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
            ExpectedResponse.ExpectedResponse
          case Some(pxValue) if pxValue < 0 =>
            queue.offer(ByteString("-ERR value for 'px' or 'ex' parameter must be a non-negative integer\r\n"))
            ExpectedResponse.NoResponse
          case _ =>
            queue.offer(ByteString("-ERR invalid value for 'px' or 'ex' parameter\r\n"))
            ExpectedResponse.NoResponse
        }
      case _ =>
        queue.offer(ByteString("-ERR wrong number of arguments for 'set' command\r\n"))
        ExpectedResponse.NoResponse
    }
  }
}
