package codecrafters_redis.commands

import codecrafters_redis.commands.logic.*

import java.io.OutputStream

enum RedisCommand(val logic: CommandDetectTrait & CommandHandler):
  case Ping extends RedisCommand(PingLogic)
  case Echo extends RedisCommand(EchoLogic)
  case Set extends RedisCommand(SetLogic)
  case Get extends RedisCommand(GetLogic)
  case Config extends RedisCommand(ConfigLogic)
