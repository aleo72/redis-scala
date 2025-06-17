package codecrafters_redis.commands.logic

import codecrafters_redis.commands.{CommandDetectTrait, CommandHandler, ProtocolMessage}

import java.io.OutputStream

object EchoLogic extends CommandDetectTrait with CommandHandler {

  override def commandName: String = "ECHO"

  override def canHandle(command: ProtocolMessage): Boolean = {
    command.bulkMessageString.equalsIgnoreCase(commandName) ||
//    command.statusCodeString.equalsIgnoreCase("ECHO") ||
    command.multiBulkMessage.flatMap(_.headOption).exists(_.bulkMessageString.equalsIgnoreCase(commandName))

  }

  override def handle(command: ProtocolMessage, out: OutputStream): Unit = {
    // Extract the message to echo from the command
    val message = command.multiBulkMessage.map(_.tail).map(_.map(_.bulkMessageString).mkString(" ")).getOrElse("")
    // Prepare the response
    val response = s"+$message\r\n"
    // Write the response to the output stream
    out.write(stringToBytes(response))
  }
}
