package codecrafters_redis.rdb

enum ParsingPhase:
  case ReadingHeader
  case ReadingOpCode
  case ReadingKeyValuePair(valueTypeCode: Int)
  case Finished
