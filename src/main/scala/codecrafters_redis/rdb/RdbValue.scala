package codecrafters_redis.rdb

enum RdbValue:
  case RdbString(value: String)
  case RdbList(items: Vector[String])
  case RdbSet(members: Set[String])
  case RdbZSet(entries: Vector[(String, Double)])
  case RdbHash(fields: Map[String, String])


