package codecrafters_redis.rdb

enum RdbOpCode(val code: Int):
  case AUX extends RdbOpCode(0xfa)
  case RESIZEDB extends RdbOpCode(0xfb)
  case EXPIRETIME_MS extends RdbOpCode(0xfc)
  case EXPIRETIME extends RdbOpCode(0xfd)
  case SELECTDB extends RdbOpCode(0xfe)
  case EOF extends RdbOpCode(0xff)


