package codecrafters_redis.logic.commands

import codecrafters_redis.exceptions.{
  RedisAskDataException,
  RedisBusyException,
  RedisClusterException,
  RedisInputStreamException,
  RedisMovedDataException
}
import codecrafters_redis.util.{RedisInputStream, SafeEncoder}

import Protocol.Binary
case class ProtocolMessage(
    statusCode: Option[Binary] = None,
    bulkMessage: Option[Binary] = None,
    multiBulkMessage: Option[Seq[ProtocolMessage]] = None,
    integer: Option[Long] = None
) {
  override def toString: String = {
    s"ProtocolMessage(statusCode=${statusCode.map(SafeEncoder.encode)}, " +
      s"bulkMessage=${bulkMessage.map(SafeEncoder.encode)}, " +
      s"multiBulkMessage=${multiBulkMessage.map(_.map(_.toString))}, " +
      s"integer=$integer)"
  }
}

object Protocol {

  type Binary = Array[Byte]

  val DOLLAR_BYTE = '$'
  val ASTERISK_BYTE = '*'
  val PLUS_BYTE = '+'
  val MINUS_BYTE = '-'
  val COLON_BYTE = ':'

  val BYTES_TRUE: Binary = toByteArray(1)
  val BYTES_FALSE: Binary = toByteArray(0)
  val BYTES_TILDE: Binary = SafeEncoder.encode("~")
  val BYTES_EQUAL: Binary = SafeEncoder.encode("=")
  val BYTES_ASTERISK: Binary = SafeEncoder.encode("*")

  val POSITIVE_INFINITY_BYTES: Binary = "+inf".getBytes
  val NEGATIVE_INFINITY_BYTES: Binary = "-inf".getBytes

  private val ASK_PREFIX = "ASK "
  private val MOVED_PREFIX = "MOVED "
  private val CLUSTERDOWN_PREFIX = "CLUSTERDOWN "
  private val BUSY_PREFIX = "BUSY "
  private val NOSCRIPT_PREFIX = "NOSCRIPT "
  private val WRONGPASS_PREFIX = "WRONGPASS"
  private val NOPERM_PREFIX = "NOPERM"

  def read(is: RedisInputStream): Option[ProtocolMessage] = {
//    is.readLine() match {
//      case null => throw new IllegalArgumentException("Input stream is null")
//      case line if line.startsWith("*") => readArray(is, line)
//      case line if line.startsWith("$") => readBulkString(is, line)
//      case line if line.startsWith(":") => readInteger(line)
//      case line if line.startsWith("+") => readSimpleString(line)
//      case line if line.startsWith("-") => readError(line)
//      case _ => throw new IllegalArgumentException(s"Unknown protocol format: $line")
//    }
    process(is)
  }

  def process(is: RedisInputStream): Option[ProtocolMessage] = {
    val b = is.readByte()
    b match {
      case PLUS_BYTE     => processStatusCodeReply(is)
      case DOLLAR_BYTE   => processBulkReply(is)
      case ASTERISK_BYTE => processMultiBulkReply(is)
      case COLON_BYTE    => processInteger(is)
      case MINUS_BYTE =>
        processError(is)
        None
      case _ =>
        throw new IllegalArgumentException(s"Unknown protocol format: $b")
    }
  }

  def processError(is: RedisInputStream): Unit = {
    val line: String = is.readLine()
    if (line.startsWith(MOVED_PREFIX)) {
      val info = parseTargetHostAndSlot(line)
      throw new RedisMovedDataException(
        line,
        info._1.toIntOption.getOrElse(-1),
        info._2.toIntOption.getOrElse(-1)
      )
    } else if (line.startsWith(ASK_PREFIX)) {
      val info = parseTargetHostAndSlot(line)
      throw new RedisAskDataException(
        line,
        info._1.toIntOption.getOrElse(-1),
        info._2.toIntOption.getOrElse(-1)
      )
    } else if (line.startsWith(CLUSTERDOWN_PREFIX)) {
      throw new RedisClusterException(line)
    } else if (line.startsWith(BUSY_PREFIX)) {
      throw new RedisBusyException(line)
    } else if (line.startsWith(NOSCRIPT_PREFIX)) {
      throw new IllegalArgumentException(line)
    } else if (line.startsWith(WRONGPASS_PREFIX)) {
      throw new IllegalArgumentException(line)
    } else if (line.startsWith(NOPERM_PREFIX)) {
      throw new IllegalArgumentException(line)
    } else {
      throw new IllegalArgumentException(s"Unknown error response: $line")
    }
  }

  private def parseTargetHostAndSlot(
      clusterRedirectResponse: String
  ): (String, String) = {
    val messageInfo = clusterRedirectResponse.split(" ")
    messageInfo(1) -> messageInfo(2)
  }

  private def processMultiBulkReply(is: RedisInputStream): Option[ProtocolMessage] = {
    val num = is.readIntCrLf()
    if (num == -1) {
      None
    } else {

      val messages: Seq[ProtocolMessage] = (0 until num).flatMap { _ =>
        try {
          process(is)
        } catch {
          case e: RedisInputStreamException =>
            throw e
        }
      }.toVector
      Option(
        ProtocolMessage(multiBulkMessage = Option(messages))
      )
    }
  }

  private def processStatusCodeReply(is: RedisInputStream): Option[ProtocolMessage] =
    Option(
      ProtocolMessage(
        statusCode = Some(is.readLine().getBytes)
      )
    )

  private def processBulkReply(is: RedisInputStream): Option[ProtocolMessage] = {
    val length = is.readIntCrLf()
    if (length < 0) None
    else {
      val result = Option(
        ProtocolMessage(
          bulkMessage = Some(readFully(is, length))
        )
      )
      is.readDelimiter()
      result
    }
  }

  def readFully(is: RedisInputStream, len: Int): Binary = {
    val read = new Array[Byte](len)
    @annotation.tailrec
    def loop(offset: Int): Unit = {
      if (offset < len) {
        val size = is.read(read, offset, len - offset)
        if (size == -1) {
          throw new RedisInputStreamException(
            "It seems like server has closed the connection."
          )
        }
        loop(offset + size)
      }
    }
    loop(0)
    read
  }

  def processInteger(is: RedisInputStream): Option[ProtocolMessage] = Option(
    ProtocolMessage(
      integer = Some(is.readLongCrLf())
    )
  )

  def toByteArray(value: Boolean): Array[Byte] =
    if (value) BYTES_TRUE
    else BYTES_FALSE

  def toByteArray(value: Int): Array[Byte] =
    SafeEncoder.encode(String.valueOf(value))

  def toByteArray(value: Long): Array[Byte] =
    SafeEncoder.encode(String.valueOf(value))

}
