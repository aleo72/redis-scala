package codecrafters_redis.rdb.parser

enum OptCode(val code: Int):
  case EOF extends OptCode(0xff)
  case SELECT_DB extends OptCode(0xfe)
  case EXPIRETIME extends OptCode(0xfd)
  case EXPIRETIME_MS extends OptCode(0xfc)
  case RESIZE_DB extends OptCode(0xfb)
  case AUX extends OptCode(0xfa)
  case FREQ extends OptCode(0xf9)
  case IDLE extends OptCode(0xf8)
  case MODULE_AUX extends OptCode(0xf7)
  case FUNCTION_PRE_GA extends OptCode(0xf6)
  case FUNCTION2 extends OptCode(0xf5)
  case SLOT_INFO extends OptCode(0xf4)

  case __UNKNOWN__ extends OptCode(0x00)

object OptCode:
  def fromCode(code: Int): OptCode = values.find(_.code == code).getOrElse(__UNKNOWN__)
