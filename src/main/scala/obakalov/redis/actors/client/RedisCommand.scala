package obakalov.redis.actors.client

import obakalov.redis.actors.client.logic.*

sealed trait RedisCommand:
  def logic: CommandDetectTrait

object RedisCommand:
  val values: Array[RedisCommand] = RedisDatabaseCommand.values ++
    RedisSimpleCommand.values ++
    RedisReplicationCommand.values

enum RedisDatabaseCommand(val logic: CommandDetectTrait & DatabaseCommandHandler) extends RedisCommand:
  case Set extends RedisDatabaseCommand(SetLogic)
  case Get extends RedisDatabaseCommand(GetLogic)
  case Config extends RedisDatabaseCommand(ConfigLogic)
  case Keys extends RedisDatabaseCommand(KeysLogic)

enum RedisSimpleCommand(val logic: CommandDetectTrait & SimpleCommandHandler) extends RedisCommand:
  case PING extends RedisSimpleCommand(PingLogic)
  case ECHO extends RedisSimpleCommand(EchoLogic)

enum RedisReplicationCommand(val logic: CommandDetectTrait & ReplicationCommandHandler) extends RedisCommand:
  case INFO extends RedisReplicationCommand(ReplicationInfoLogic)
