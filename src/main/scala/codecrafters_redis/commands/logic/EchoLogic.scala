package codecrafters_redis.commands.logic

import codecrafters_redis.commands.{CommandDetectTrait, CommandHandler, ProtocolMessage}

import java.io.OutputStream

object EchoLogic extends CommandDetectTrait with CommandHandler {

  override def commandName: String = "ECHO"

  override def handle(command: ProtocolMessage, out: OutputStream): Unit = {
    // Extract the message to echo from the command
    val message = command.multiBulkMessage.map(_.tail).map(_.map(_.bulkMessageString).mkString(" ")).getOrElse("")
    // Write the response to the output stream
    out.write(responseToBytes(s"+$message"))
  }
}
