package obakalov.redis.actors.client

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
  
  def simpleString(data: String): String = {
    s"+$data\r\n"
  }
}
