package codecrafters_redis.commands

import codecrafters_redis.commands.logic.{EchoLogic, PingLogic}

import java.io.OutputStream

enum RedisCommand(val logic: CommandDetectTrait & CommandHandler):
  case Ping extends RedisCommand(PingLogic)
  case Echo extends RedisCommand(EchoLogic)
