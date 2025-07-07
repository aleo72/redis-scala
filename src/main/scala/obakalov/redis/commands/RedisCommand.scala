package obakalov.redis.commands

import obakalov.redis.commands.logic.{ConfigLogic, EchoLogic, GetLogic, KeysLogic, PingLogic, SetLogic}

import java.io.OutputStream

enum RedisCommand(val logic: CommandDetectTrait & CommandHandler):
  case Ping extends RedisCommand(PingLogic)
  case Echo extends RedisCommand(EchoLogic)
  case Set extends RedisCommand(SetLogic)
  case Get extends RedisCommand(GetLogic)
  case Config extends RedisCommand(ConfigLogic)
  case Keys extends RedisCommand(KeysLogic)
