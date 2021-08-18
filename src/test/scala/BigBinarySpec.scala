import org.scalatest._
import util._
import combinatorics._


class BigBinarySpec extends FlatSpec with Matchers {
  val x1 = BigBinary(Big.pow2(60) - 1)

  "Conversion to char array" should "work" in {
    val a = x1.toCharArray(60).map(_.toInt)
    assert(a.toList == List(255,255,255,255,255,255,255,15))
  }
}



