package obakalov.redis.rdb.models

enum RdbFileEntry:
  case RedisKeyValue(key: String, value: RdbValue, dbNumber: Int)
  case ResizeDb(dbNumber: Int, dbHashTableSize: Long, expireTimeHashTableSize: Long)
  case AuxField(key: String, value: String)

case class RdbHeader(
    version: Int,
    dbNumber: Int,
    dbHashTableSize: Long,
    expireTimeHashTableSize: Long,
    auxFields: Map[String, String]
) {
  val v: RdbValue = RdbValue.RdbBinary(Array.emptyByteArray, None) // Placeholder for RdbValue

}

enum RdbValue(val expireAt: Option[Long]):
  case RdbBinary(val value: Array[Byte], override val expireAt: Option[Long]) extends RdbValue(expireAt)
  case RdbInt(val value: Long, override val expireAt: Option[Long]) extends RdbValue(expireAt)
  case RdbList(val items: Vector[Array[Byte]], override val expireAt: Option[Long]) extends RdbValue(expireAt)
  case RdbSet(val members: Set[Array[Byte]], override val expireAt: Option[Long]) extends RdbValue(expireAt)
  case RdbZSet(val entries: Vector[(String, Double)], override val expireAt: Option[Long]) extends RdbValue(expireAt)
  case RdbHash(val fields: Map[String, String], override val expireAt: Option[Long]) extends RdbValue(expireAt)
