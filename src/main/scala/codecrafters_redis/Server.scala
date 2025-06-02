package codecrafters_redis

import java.io.OutputStream
import java.net.{InetSocketAddress, ServerSocket}
import java.nio.charset.StandardCharsets

object Server {
  def main(args: Array[String]): Unit = {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println("Logs from your program will appear here!")

    // Uncomment this to pass the first stage
    //
     val serverSocket = new ServerSocket()
     serverSocket.bind(new InetSocketAddress("localhost", 6379))
     val clientSocket = serverSocket.accept() // wait for client

    println("Server is running and waiting for a client to connect...")

    val inputStream = clientSocket.getInputStream
    val outputStream: OutputStream = clientSocket.getOutputStream

    // Read the first byte from the input stream
    val inputArray = new Array[Byte](1024) // 1 KB
    inputStream.read(inputArray)
    val inputString = new String(inputArray, StandardCharsets.UTF_8).trim
    println(s"Received input: $inputString")
    // Process the input and send a response
    inputString.split("\r\n").headOption match {
      case Some(command) =>
        println(s"Processing command: $command")
        // Call the function to process the command
        processCommand(command, outputStream)
      case None =>
        println("No command received.")
    }


    // Close the streams and socket
    outputStream.close()
    inputStream.close()
    clientSocket.close()
    serverSocket.close()

    println("Server has finished processing the request and is shutting down.")
  }

  def processCommand(command: String, out: OutputStream): Unit = {
    // This function can be extended to handle more commands
   log(s"Processing command: $command")
    command match {
      case "PING" => out.write("+PONG\r\n".getBytes(StandardCharsets.UTF_8));
      case _ =>
        log(s"Unknown command: $command")
//        "-ERR unknown command\r\n"
    }
  }

  def log(message: String): Unit = {
    // This function can be used to log messages
    println(s"LOG: $message")
  }
}
