package codecrafters_redis.rdb.parser

enum OpCode(val code: Int):
  case EOF extends OpCode(0xff)
  case SELECT_DB extends OpCode(0xfe)
  case EXPIRETIME extends OpCode(0xfd)
  case EXPIRETIME_MS extends OpCode(0xfc)
  case RESIZE_DB extends OpCode(0xfb)
  case AUX extends OpCode(0xfa)
  case FREQ extends OpCode(0xf9)
  case IDLE extends OpCode(0xf8)
  case MODULE_AUX extends OpCode(0xf7)
  case FUNCTION_PRE_GA extends OpCode(0xf6)
  case FUNCTION2 extends OpCode(0xf5)
  case SLOT_INFO extends OpCode(0xf4)

  // Represents a key-value entry in the RDB file.
  case ENTRY_KEY_VALUE extends OpCode(0)
  case ENTRY_LIST extends OpCode(1)
  case ENTRY_SET extends OpCode(2)
  case ENTRY_SORTED_SET extends OpCode(3)
  case ENTRY_HASH extends OpCode(4)
  case ENTRY_SORTED_SET_2 extends OpCode(5)
  case ENTRY_MODULE_V1 extends OpCode(6) // not supported
  case ENTRY_MODULE_V2 extends OpCode(7) // not supported

object OpCode:
  def fromCode(code: Int): OpCode = values
    .find(_.code == code)
    .getOrElse(
      throw new IllegalArgumentException(s"Unknown OpCode: $code")
    )
