package obakalov.redis.actors.client

import obakalov.redis.actors.client.logic.*


enum RedisCommand(val handler: CommandHandler & CommandDetectTrait):
  case SET extends RedisCommand(SetLogic)
  case GET extends RedisCommand(GetLogic)
  case CONFIG extends RedisCommand(ConfigLogic)
  case KEYS extends RedisCommand(KeysLogic)
  case PING extends RedisCommand(PingLogic)
  case ECHO extends RedisCommand(EchoLogic)
  case INFO extends RedisCommand(ReplicationInfoLogic)
  case REPLCONF extends RedisCommand(ReplConfLogic)
  case PSYNC extends RedisCommand(PsyncLogic)

  def commandName: String = handler.commandName
