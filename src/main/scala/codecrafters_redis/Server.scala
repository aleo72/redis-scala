package codecrafters_redis

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
    val outputStream = clientSocket.getOutputStream

    // Read the first byte from the input stream
    val inputArray = new Array[Byte](1024) // 1 KB
    inputStream.read(inputArray)
    val inputString = new String(inputArray, StandardCharsets.UTF_8).trim
    println(s"Received input: $inputString")
    // Process the input and send a response
    inputString match {
      case "PING" =>
        val response = "+PONG\r\n"
        outputStream.write(response.getBytes(StandardCharsets.UTF_8))
        println("Sent response: PONG")
      case _ =>
        val response = "-ERR unknown command\r\n"
        outputStream.write(response.getBytes(StandardCharsets.UTF_8))
        println("Sent response: ERR unknown command")
    }
  }
}
