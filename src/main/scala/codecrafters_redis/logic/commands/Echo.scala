package codecrafters_redis.logic.commands

import java.io.OutputStream

object Echo extends CommandDetectTrait with CommandHandler {

  override def canHandle(command: String): Boolean = {
    command.toLowerCase.startsWith("echo")
  }

  override def handle(out: OutputStream): Unit = {
    // Extract the message to echo from the command
    val message = out.toString.split(" ", 2).lift(1).getOrElse("")
    // Prepare the response
    val response = s"+$message\r\n"
    // Write the response to the output stream
    out.write(stringToBytes(response))
  }
}
