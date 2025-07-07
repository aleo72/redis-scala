package obakalov.redis.util

import obakalov.redis.exceptions.RedisInputStreamException
import java.io.{FilterInputStream, InputStream}
import scala.annotation.tailrec

class RedisInputStream(is: InputStream) extends FilterInputStream(is) {

  private val INPUT_BUFFER_SIZE: Int = 8192 // todo read from config

  private val buffer: Array[Byte] = new Array[Byte](INPUT_BUFFER_SIZE)
  private var count, limit: Int = 0

  def this(is: InputStream, bufferSize: Int) = {
    this(is)
  }

  def readByte(): Byte = {
    ensureFill()
    val bytesRead = buffer(count)
    count += 1
    bytesRead
  }

  def readLine(): String = readLineTailRec(new StringBuilder)

  @annotation.tailrec
  private def readLineTailRec(acc: StringBuilder): String = {
    val b = readByte()
    if (b == '\r') {
      val c = readByte()
      if (c == '\n') {
        val reply = acc.toString()
        if (reply.isEmpty)
          throw new RedisInputStreamException("Empty line read from stream")
        else
          reply
      } else {
        readLineTailRec(acc.append(b.toChar).append(c.toChar))
      }
    } else {
      readLineTailRec(acc.append(b.toChar))
    }
  }

  def readLineBytes(): Array[Byte] = {
    ensureFill()
    val start = count
    // Recursive function to scan for the end-of-line (\r\n)
    @tailrec
    def scan(pos: Int): Option[Int] = {
      if (pos >= limit) None
      else if (buffer(pos) == '\r') {
        if (pos + 1 >= limit) None
        else if (buffer(pos + 1) == '\n') Some(pos + 2)
        else scan(pos + 1)
      } else scan(pos + 1)
    }

    scan(start) match {
      case Some(pos) =>
        val n = (pos - start) - 2
        val line = new Array[Byte](n)
        System.arraycopy(buffer, start, line, 0, n)
        count = pos
        line
      case None =>
        readLineBytesSlowly()
    }
  }

  // Functional slow path
  def readLineBytesSlowly(): Array[Byte] = {
    // Tail-recursive accumulator function
    @annotation.tailrec
    def loop(acc: Vector[Byte]): Vector[Byte] = {
      ensureFill()
      val b = buffer(count)
      count += 1
      if (b == '\r') {
        ensureFill()
        val c = buffer(count)
        count += 1
        if (c == '\n') acc
        else loop(acc :+ b :+ c)
      } else {
        loop(acc :+ b)
      }
    }

    loop(Vector.empty).toArray
  }

  def readIntCrLf(): Int = readLongCrLf().toInt

  /** Reads the next two bytes from the stream, which are expected to be a delimiter (e.g., "\r\n").
    * This method does not check for the correctness of the delimiter.
    */
  def readDelimiter(): Unit = {
    this.readByte()
    this.readByte()
  }

  def readLongCrLf(): Long = {
    ensureFill()

    val isNeg = buffer(count) == '-'
    if (isNeg) count += 1

    // Tail-recursive function to accumulate value
    @annotation.tailrec
    def loop(value: Long): Long = {
      ensureFill()
      val b = buffer(count)
      count += 1
      if (b == '\r') {
        ensureFill()
        if (buffer(count) != '\n')
          throw new RedisInputStreamException("Unexpected character!")
        count += 1
        value
      } else {
        loop(value * 10 + (b - '0'))
      }
    }

    val result = loop(0L)
    if (isNeg) -result else result
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    ensureFill()
    val length = math.min(limit - count, len)
    System.arraycopy(buffer, count, b, off, length)
    count += length
    length
  }

  private def ensureFill(): Unit = {
    if (count >= limit) {
      try {
        limit = in.read(buffer)
        count = 0
        if (limit == -1) {
          throw new RedisInputStreamException("End of stream reached")
        }
      } catch {
        case e: Exception => throw new RedisInputStreamException(e)
      }
    }
  }
}
