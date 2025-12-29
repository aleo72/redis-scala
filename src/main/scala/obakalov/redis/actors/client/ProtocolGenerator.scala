package obakalov.redis.actors.client

import org.apache.pekko.util.ByteString

object ProtocolGenerator {

  def generateBulkString(data: String*): String = {
    val sb = new StringBuilder
    sb.append(s"*${data.length}\r\n")
    data.foreach { str =>
      sb.append(s"$$${str.getBytes.length}\r\n")
      sb.append(s"$str\r\n")
    }
    sb.toString()
  }

  def createBulkString(v: Array[Byte]): ByteString = {
    if (v == null) ByteString("$-1\r\n")
    else ByteString("$") ++ ByteString(v.length.toString) ++ ByteString("\r\n") ++ ByteString(v) ++ ByteString("\r\n")
  }

  def simpleString(data: String): String = {
    s"+$data\r\n"
  }

  def generateRespArray(args: ByteString*): ByteString = {
    val header = ByteString(s"*${args.length}\r\n")
    args.foldLeft(header) { (acc, arg) =>
      acc ++ ByteString(s"$$${arg.length}\r\n") ++ arg ++ ByteString("\r\n")
    }
  }
}
