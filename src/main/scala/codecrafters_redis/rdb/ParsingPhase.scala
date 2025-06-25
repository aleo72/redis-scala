package codecrafters_redis.rdb

enum ParsingPhase:
  case ReadingHeader
  case ReadingOpCode
  case ReadingValue
  case Finished
