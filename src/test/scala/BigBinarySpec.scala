import org.scalatest._
import util._
import combinatorics._


class BigBinarySpec extends FlatSpec with Matchers {
  val x1 = BigBinary(Big.pow2(60) - 1)

  "Conversion to char array" should "work" in {
    val a = x1.toCharArray(60).map(_.toInt)
    assert(a.toList == List(255,255,255,255,255,255,255,15))
  }

  "Pup " should "work for ranges " in {
    val tests = List((15, 2 to 5, 60), (5, 4 to 6, 80), (12, 5 to 7, 4 * 32), (12, 5 to 6, 0), (3, 2 to 7, 12), (0, 5 to 9, 0))
    tests.foreach{ case (i1, r, i2) => assert(BigBinary(i1).pup(r) === BigBinary(i2))}
  }

  "UnPup " should "work for ranges " in {
    val tests = List((15, 2 to 5, 60), (5, 4 to 6, 80), (4, 5 to 7, 4 * 32), (0, 5 to 6, 0), (3, 2 to 7, 12), (0, 5 to 9, 0))
    tests.foreach{ case (i1, r, i2) => assert(BigBinary(i2).unpup(r) === BigBinary(i1))}
  }


  "Pup " should "work for range seq " in {
    val tests = List((15, List(2,3,4,5), 60), (5, List(4,5,6), 80), (12, List(5,6,7), 4 * 32), (12, List(5,6), 0), (3, List(2,3,4,5,6,7), 12), (0, List(5,6,7,8,9), 0))
    tests.foreach{ case (i1, r, i2) => assert(BigBinary(i1).pup(r) === BigBinary(i2))}
  }
  "UnPup " should "work for range seq " in {
    val tests = List((15, List(2,3,4,5), 60 + 384), (5, List(4,5,6), 80 + 384), (4, List(5,6,7), 12 * 32 + 1024), (3, List(2,3,4,5,6,7), 12 + 1024), (0,  List(5,6,7,8,9), 1024))
    tests.foreach{ case (i1, r, i2) => assert(BigBinary(i2).unpup(r) === BigBinary(i1))}
  }

  "Pup" should "work for arbitrary seq" in {
    val tests = List((15, List(1,5,2,7), 166), (1024+9, List(1,5,2,7), 130), (32+3,List(1,5,2,7), 34), (32+12, List(1,5,2,7), 132))
    tests.foreach{ case (i1, r, i2) => assert(BigBinary(i1).pup(r) === BigBinary(i2))}
  }

  "UnPup" should "work for arbitrary seq" in {
    val tests = List((15, List(1,5,2,7), 166+1024), (9, List(1,5,2,7), 130+1024), (3,List(1,5,2,7), 34+1024), (12, List(1,5,2,7), 132+1024))
    tests.foreach{ case (i1, r, i2) => assert(BigBinary(i2).unpup(r) === BigBinary(i1))}
  }
}



