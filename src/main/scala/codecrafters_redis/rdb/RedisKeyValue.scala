package codecrafters_redis.rdb

case class RedisKeyValue(
    key: String,
    value: RdbValue,
    expireAt: Option[Long] // Unix timestamp in milliseconds
)
