package codecrafters_redis.rdb

import akka.util.ByteString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ParsedLengthSpec extends AnyWordSpec with Matchers {

  "ParsedLength.parse" should {
    "parse a simple 6-bit length" in {
      val data = ByteString(10) // 00001010
      ParsedLength.parse(data) shouldBe Some(ParsedLength(10, 1))
    }

    "parse the maximum 6-bit length" in {
      val data = ByteString(63) // 00111111
      ParsedLength.parse(data) shouldBe Some(ParsedLength(63, 1))
    }

    "parse a 14-bit length" in {
      // Длина 500 (в hex 0x1F4).
      // Для кодирования в 14-битном формате нужно два байта (b1, b2).
      // val = ((b1 & 0x3F) << 8) | b2
      // Для val = 500:
      // b1 = ((500 >> 8) & 0x3F) | 0x40 = (1 & 0x3F) | 0x40 = 1 | 64 = 65 (0x41)
      // b2 = 500 & 0xFF = 244 (0xF4)
      // Таким образом, правильные байты: 0x41, 0xF4.
      val data = ByteString(0x41, 0xf4)
      ParsedLength.parse(data) shouldBe Some(ParsedLength(500, 2))
    }

    "parse the maximum 14-bit length" in {
      // Длина 16383 = 0011 1111 1111 1111. Формат: 01 111111 (0x7F) 11111111 (0xFF)
      val data = ByteString(0x7f, 0xff)
      ParsedLength.parse(data) shouldBe Some(ParsedLength(16383, 2))
    }

    "parse a 32-bit length" in {
      // Длина 70000. Флаг: 10000000 (0x80). Число: 00000000 00000001 00010001 01110000
      val data = ByteString(0x80, 0x00, 0x01, 0x11, 0x70)
      ParsedLength.parse(data) shouldBe Some(ParsedLength(70000, 5))
    }

    "return None if buffer is too small for a 14-bit length" in {
      val data = ByteString(0x47) // Только первый байт
      ParsedLength.parse(data) shouldBe None
    }

    "return None if buffer is too small for a 32-bit length" in {
      val data = ByteString(0x80, 0x01, 0x02) // Только 3 байта из 5
      ParsedLength.parse(data) shouldBe None
    }

    "return None for an empty buffer" in {
      ParsedLength.parse(ByteString.empty) shouldBe None
    }

    "throw an exception for the special encoding format (0x03)" in {
      val data = ByteString(0xc0) // 11000000
      assertThrows[Exception] {
        ParsedLength.parse(data)
      }
    }
    "correctly parse length from a buffer with extra data" in {
      // Проверяем, что парсер потребляет только нужные байты
      val data = ByteString(10, 1, 2, 3) // Длина 10 + мусор
      ParsedLength.parse(data) shouldBe Some(ParsedLength(10, 1))
    }
  }
}
