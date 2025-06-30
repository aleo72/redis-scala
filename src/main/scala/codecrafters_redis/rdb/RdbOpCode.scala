package codecrafters_redis.rdb

enum RdbOpCode(val code: Int):
  case AUX extends RdbOpCode(0xfa)
  case RESIZE_DB extends RdbOpCode(0xfb)
  case EXPIRETIME_MS extends RdbOpCode(0xfc)
  case EXPIRETIME extends RdbOpCode(0xfd)
  case SELECT_DB extends RdbOpCode(0xfe)
  case EOF extends RdbOpCode(0xff)

  case STRING extends RdbOpCode(0x00)
  case LIST extends RdbOpCode(0x01)
  case SET extends RdbOpCode(0x02)
  case ZSET extends RdbOpCode(0x03)
  case HASH extends RdbOpCode(0x04)

  case ENC_INT8 extends RdbOpCode(0xc0)
  case ENC_INT16 extends RdbOpCode(0xc1)
  case ENC_INT32 extends RdbOpCode(0xc2)
  case ENC_LZF extends RdbOpCode(0xc3) // LZF compressed string
