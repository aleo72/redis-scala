package codecrafters_redis.util

import java.nio.charset.StandardCharsets

object SafeEncoder {
  val DEFAULT_CHARSET = StandardCharsets.UTF_8


  def encodeMany(strs: String*): Array[Array[Byte]] = {
    val many = new Array[Array[Byte]](strs.length)
    for (i <- 0 until strs.length) {
      many(i) = encode(strs(i))
    }
    many
  }

  def encode(str: String): Array[Byte] = {
    if (str == null) throw new IllegalArgumentException("null value cannot be sent to redis")
    str.getBytes(DEFAULT_CHARSET)
  }

  def encode(data: Array[Byte]) = new String(data, DEFAULT_CHARSET)

/*  /**
   * This method takes an object and will convert all bytes[] and list of byte[] and will encode the
   * object in a recursive way.
   *
   * @param dataToEncode
   * @return the object fully encoded
   */
  def encodeObject(dataToEncode: AnyRef): AnyRef = {
    dataToEncode match {
      case null => throw new IllegalArgumentException("null value cannot be sent to redis")
      case bytes: Array[Byte] => encode(bytes)
      case list: List[_] =>
        list.map {
          case b: Array[Byte] => encode(b)
          case other => other
        }
      case _ => dataToEncode
    }
  }*/
}
