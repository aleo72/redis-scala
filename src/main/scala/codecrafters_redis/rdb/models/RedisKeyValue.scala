package codecrafters_redis.rdb.models

enum RedisEntry:
  case RedisKeyValue(key: String, value: RdbValue, expireAt: Option[Long], dbNumber: Int)
  case ResizeDb(dbNumber: Int, dbHashTableSize: Long, expireTimeHashTableSize: Long)
  case AuxField(key: String, value: String)

enum RdbValue:
  case RdbValue(value: Array[Byte])
//  case RdbString(value: Array[Byte])
  case RdbInt(value: Long)
  case RdbList(items: Vector[Array[Byte]])
  case RdbSet(members: Set[Array[Byte]])
  case RdbZSet(entries: Vector[(String, Double)])
  case RdbHash(fields: Map[String, String])
