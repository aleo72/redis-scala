package codecrafters_redis.rdb

import akka.util.ByteString

import java.nio.ByteOrder.BIG_ENDIAN
case class ParsedLength(value: Int, consumeBytes: Int)

object ParsedLength {

  val `00` = 0x00
  val `01` = 0x01
  val `02` = 0x02
  val `03` = 0x03

  def parse(buffer: ByteString): Option[ParsedLength] = {
    if (buffer.isEmpty) None
    else {
      val firstByte = buffer.head.toInt & 0xff
      val twoBitFlag = firstByte >> 6
      twoBitFlag match {
        case `00` => // Case 1: 00xxxxxx -> 6-bit length number
          val length = firstByte & 0x3f
          Some(ParsedLength(length, 1))
        case `01` => // Case 2: 01xxxxxx -> 14-bit length number
          if (buffer.length < 2) None
          else {
            val part1 = (firstByte & 0x3f) << 8
            val part2 = buffer(1).toInt & 0xff
            val length = part1 | part2 // union of two bytes
            Some(ParsedLength(length, 2))
          }
        case `02` => // Case 3: 10xxxxxx -> 32-bit length number
          if (buffer.length < 5) None
          else {
            val length = buffer.drop(1).iterator.getInt(using BIG_ENDIAN)
            Some(ParsedLength(length, 5))
          }
        case `03` => // Case 4: 11xxxxxx -> special case, not length
          throw new RuntimeException("Special case 11xxxxxx is not a length")
      }
    }
  }
}
