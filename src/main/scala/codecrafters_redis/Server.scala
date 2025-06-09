package codecrafters_redis

import java.io.{BufferedReader, Closeable, OutputStream}
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
    val outputStream: OutputStream = clientSocket.getOutputStream
    log("Server is running and waiting for a client to connect...")

    val reader = createReader(clientSocket.getInputStream)

    while (true) {
      val command = reader.readLine()
      if (command == null)
        shutdown(serverSocket, clientSocket)
      log(s"Received command: $command")
      processCommand(command, clientSocket.getOutputStream)
    }

    shutdown(serverSocket, clientSocket)
  }

  private def processCommand(command: String, out: OutputStream): Unit = {
    // This function can be extended to handle more commands
    log(s"Processing command: $command")
    command match {
      case "PING" => out.write("+PONG\r\n".getBytes(StandardCharsets.UTF_8));
      case _ =>
        log(s"Unknown command: $command")
//        "-ERR unknown command\r\n"
    }
  }

  def createReader(inputStream: java.io.InputStream): BufferedReader =
    new BufferedReader(
      new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)
    )

  def log(message: String): Unit = {
    // This function can be used to log messages
    println(s"LOG: $message")
  }

  def shutdown(
      serverSocket: ServerSocket,
      clientSocket: java.net.Socket
  ): Unit = {
    // This function can be used to gracefully shut down the server
    log("Shutting down the server...")
    log("Client disconnected, shutting down server...")
    closeStreams(serverSocket, "ServerSocket")
    closeStreams(clientSocket, "ClientSocket")
    sys.exit(0)

  }

  def closeStreams(s: Closeable, name: String): Unit = {
    try {
      if (s != null) {
        s.close()
        log(s"$name closed successfully.")
      }
    } catch {
      case e: Exception =>
        log(s"Error closing $name: ${e.getMessage}")
    }
  }
}
