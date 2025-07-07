package codecrafters_redis.rdb.parser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.pekko.util.ByteString

class RdbParserSpec extends AnyFlatSpec with Matchers {

  "RdbParser" should "correctly parse the header" in {
    val buffer = ByteString(
      Array[Byte](
        82, 69, 68, 73, 83, 48, 48, 49, 49, -6, 9, 114, 101, 100, 105, 115, 45, 118, 101, 114, 5, 55, 46, 50, 46, 48, -6, 10, 114, 101, 100, 105, 115, 45, 98,
        105, 116, 115, -64, 64, -2, 0, -5, 1, 0, 0, 9, 112, 105, 110, 101, 97, 112, 112, 108, 101, 6, 98, 97, 110, 97, 110, 97, -1, -97, -92, 112, 57, -115, 74,
        85, 102
      )
    )
    var initialState = ParserState(step = ParsingStep.ReadingHeader, buffer = buffer)

    for (i <- 0 until 8) {
      val r = RdbParser.readNext(state = initialState)
      println(s"|Step: ${r.state.step}\n|Buffer: ```${r.state.buffer.utf8String}```\n|Buffer bytes: ${r.state.buffer}")
      println(s"|Bytes consumed: ${r.state.bytesConsumed}, Version: ${r.state.version}")
      println(s"|Current DB: ${r.state.currentDb}, Next expiry: ${r.state.nextExpiry}")
      println(s"|isContinue: ${r.isContinue}, Entry: ${r.entry}")
      println("-----------------------------------")
      initialState = r.state
    }

  }
}
