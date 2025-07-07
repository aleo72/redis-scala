package obakalov.redis.exceptions

class RedisInputStreamException(message: String, cause: Throwable)
    extends Throwable(message, cause) {
  def this(message: String) = this(message, null)
  def this(cause: Throwable) = this(null, cause)

  override def getMessage: String = {
    if (message != null) message
    else if (cause != null) cause.getMessage
    else "Unknown Redis Input Stream Exception"
  }
}
