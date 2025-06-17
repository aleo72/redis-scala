package codecrafters_redis.exceptions

class RedisAskDataException (
    message: String,
    port1: Int,
    port2: Int,
    cause: Throwable
) extends Throwable(message, cause) {
  def this(message: String, port1: Int, port2: Int) =
    this(message, port1, port2, null)
  def this(port1: Int, port2: Int) = this(null, port1, port2, null)
  def this(cause: Throwable) = this(null, -1, -1, cause)

  override def getMessage: String = {
    if (message != null) message
    else if (cause != null) cause.getMessage
    else s"Redis ask data exception between ports $port1 and $port2"
  }

}
