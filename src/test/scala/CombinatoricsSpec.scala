import org.scalatest._

class CombinatoricsSpec extends FlatSpec with Matchers {
  import combinatorics._
  import DF._
  import Combinatorics._

  "Tests" should "work" in {
    assert(compute_df(3, List(List(0,1), List(0,2))) == 2)
    assert(compute_df(3, List(List(0,1), List(0,2), List(1,2))) == 1)
    assert(mk_comb(List("A","B","C"), 2) ==
           List(List("A", "B"), List("A", "C"), List("B", "C")))
    assert(mk_comb(4,2) == List(List(0, 1), List(0, 2), List(0, 3),
                                List(1, 2), List(1, 3), List(2, 3)))
  }
}



