import org.scalatest._
import core._
import SolverTools._
import backend.Payload
import planning.ProjectionMetaData


class SolverSpec extends FlatSpec with Matchers {
  val v = Array(2,2,2,2,2,2,2,2).map(x => new Payload(x, None))

  "removeSolved() test 1" should "work" in {
    val b = SolverTools.mk_all_non_neg[Double](1 << 3)
    val s = Solver(3, b, List(List(0,1), List(1,2)), v)
    s.gauss
    s.simplex_add
    s.compute_det_bounds

    assert(SolverTools.removeSolved(s.M, s.free_vars)._1 ==
      Vector(0, 1, 2, 3, 4, 5, 6, 7))
  }

  "removeSolved() test 2" should "work" in {
    val b = SolverTools.mk_all_non_neg[Double](1 << 3)
    val s = Solver(3, b, List(List(0,1), List(1,2)), v)
    s.gauss
    s.simplex_add
    s.compute_det_bounds

    s.M(2,0) = 0
    s.gauss
    assert(SolverTools.removeSolved(s.M, s.free_vars)._1 ==
      Vector(0, 1, 2, 3, 5, 6, 7))
  }

  "Solver test 1" should "work" in {
    val b = util.Util.mkAB[Interval[Double]](8, _ => Interval(None, None))

    // the solution is: Array(1,7,3,0,10,2,4,20)
    val v = Array[Double](11,9,7,20,8,3,12,24).map(x => new Payload(x, None))
    val s = Solver(3, b, List(List(0,1), List(1,2)), v)

    assert(s.bounds.toList == List(Interval(None,Some(8.0)), Interval(None,Some(8.0)), Interval(None,Some(3.0)), Interval(None,Some(3.0)), Interval(None,Some(11.0)), Interval(None,Some(9.0)), Interval(None,Some(7.0)), Interval(None,Some(20.0))))

    s.compute_det_bounds

    assert(s.bounds.toList == List(Interval(None,Some(8.0)), Interval(Some(0.0),Some(8.0)), Interval(None,Some(3.0)), Interval(Some(0.0),Some(3.0)), Interval(Some(3.0),Some(11.0)), Interval(None,Some(9.0)), Interval(Some(4.0),Some(7.0)), Interval(None,Some(20.0))))

    s.simplex_add

    assert(s.bounds.toList == List(Interval(Some(-1.0),Some(8.0)), Interval(Some(0.0),Some(8.0)), Interval(Some(-17.0),Some(3.0)), Interval(Some(0.0),Some(3.0)), Interval(Some(3.0),Some(11.0)), Interval(None,Some(9.0)), Interval(Some(4.0),Some(7.0)), Interval(None,Some(20.0))))

    s.compute_det_bounds

    assert(s.bounds.toList == List(Interval(Some(-1.0),Some(8.0)), Interval(Some(0.0),Some(8.0)), Interval(Some(-17.0),Some(3.0)), Interval(Some(0.0),Some(3.0)), Interval(Some(3.0),Some(11.0)), Interval(Some(0.0),Some(9.0)), Interval(Some(4.0),Some(7.0)), Interval(Some(0.0),Some(20.0))))


    (0 to s.n_vars - 1).foreach { v =>
        s.bounds(v) = s.bounds(v).intersect(Interval(Some(0.0), None))
    }

    assert(s.bounds.toList == List(Interval(Some(0.0),Some(8.0)), Interval(Some(0.0),Some(8.0)), Interval(Some(0.0),Some(3.0)), Interval(Some(0.0),Some(3.0)), Interval(Some(3.0),Some(11.0)), Interval(Some(0.0),Some(9.0)), Interval(Some(4.0),Some(7.0)), Interval(Some(0.0),Some(20.0))))

    s.compute_det_bounds

    assert(s.bounds.toList == List(Interval(Some(0.0),Some(8.0)), Interval(Some(0.0),Some(8.0)), Interval(Some(0.0),Some(3.0)), Interval(Some(0.0),Some(3.0)), Interval(Some(3.0),Some(11.0)), Interval(Some(1.0),Some(9.0)), Interval(Some(4.0),Some(7.0)), Interval(Some(17.0),Some(20.0))))

    s.simplex_add

    assert(s.bounds.toList == List(Interval(Some(0.0),Some(8.0)), Interval(Some(0.0),Some(8.0)), Interval(Some(0.0),Some(3.0)), Interval(Some(0.0),Some(3.0)), Interval(Some(3.0),Some(11.0)), Interval(Some(1.0),Some(9.0)), Interval(Some(4.0),Some(7.0)), Interval(Some(17.0),Some(20.0))))

    s.add(List((List(0,2), new Payload(4.0, None))))
    s.gauss

    assert(s.bounds.toList == List(Interval(Some(0.0),Some(4.0)), Interval(Some(0.0),Some(8.0)), Interval(Some(0.0),Some(3.0)), Interval(Some(0.0),Some(3.0)), Interval(Some(3.0),Some(11.0)), Interval(Some(1.0),Some(9.0)), Interval(Some(4.0),Some(7.0)), Interval(Some(17.0),Some(20.0))))

    s.simplex_add

    assert(s.bounds.toList == List(Interval(Some(1.0),Some(4.0)), Interval(Some(0.0),Some(8.0)), Interval(Some(0.0),Some(3.0)), Interval(Some(0.0),Some(3.0)), Interval(Some(3.0),Some(11.0)), Interval(Some(1.0),Some(9.0)), Interval(Some(4.0),Some(7.0)), Interval(Some(17.0),Some(20.0))))

    s.compute_det_bounds

    assert(s.bounds.toList == List(Interval(Some(1.0),Some(4.0)), Interval(Some(4.0),Some(7.0)), Interval(Some(0.0),Some(3.0)), Interval(Some(0.0),Some(3.0)), Interval(Some(7.0),Some(10.0)), Interval(Some(2.0),Some(5.0)), Interval(Some(4.0),Some(7.0)), Interval(Some(17.0),Some(20.0))))

    s.add(List((List(0), new Payload(1.0, None))))
    s.gauss

    assert(s.bounds.toList == List(Interval(Some(1.0),Some(1.0)), Interval(Some(4.0),Some(7.0)), Interval(Some(0.0),Some(3.0)), Interval(Some(0.0),Some(3.0)), Interval(Some(7.0),Some(10.0)), Interval(Some(2.0),Some(5.0)), Interval(Some(4.0),Some(7.0)), Interval(Some(17.0),Some(20.0))))

    s.compute_det_bounds

    assert(s.bounds.toList == List(Interval(Some(1.0),Some(1.0)), Interval(Some(7.0),Some(7.0)), Interval(Some(3.0),Some(3.0)), Interval(Some(0.0),Some(0.0)), Interval(Some(10.0),Some(10.0)), Interval(Some(2.0),Some(2.0)), Interval(Some(4.0),Some(4.0)), Interval(Some(20.0),Some(20.0))))
  }

  "Solver test 2" should "work" in {

    val l = List(List(2), List(1), List(0), List())
    val v = Array[Double](10, 26, 14, 22, 16, 20, 36).map(x => new Payload(x, None))
    val bounds = SolverTools.mk_all_non_neg[Double](1 << 3)
    val s = Solver(3, bounds, l, v)
    s.simplex_add
    s.compute_det_bounds

    assert(s.bounds.toList == List(Interval(Some(0.0),Some(10.0)), Interval(Some(0.0),Some(10.0)), Interval(Some(0.0),Some(10.0)), Interval(Some(0.0),Some(10.0)), Interval(Some(0.0),Some(14.0)), Interval(Some(0.0),Some(14.0)), Interval(Some(0.0),Some(16.0)), Interval(Some(0.0),Some(26.0))))

    // IMPROVEMENT
    s.compute_bounds

    assert(s.bounds.toList.toString == "List(Interval(Some(0.0),Some(10.0)), Interval(Some(0.0),Some(10.0)), Interval(Some(0.0),Some(10.0)), Interval(Some(0.0),Some(10.0)), Interval(Some(0.0),Some(14.0)), Interval(Some(0.0),Some(14.0)), Interval(Some(0.0),Some(16.0)), Interval(Some(0.0),Some(20.0)))")


    /* Note: This one has a -2 weight in the matrix!
    s.M
    res18: breeze.linalg.DenseMatrix[Double] =
    1.0   1.0   1.0   1.0  0.0   0.0  0.0  0.0  10.0
    1.0   1.0   0.0   0.0  1.0   1.0  0.0  0.0  14.0
    1.0   0.0   1.0   0.0  1.0   0.0  1.0  0.0  16.0
    -2.0  -1.0  -1.0  0.0  -1.0  0.0  0.0  1.0  -4.0
    */
  }

  "Solver test 3" should "work" in {
    // correct query result:
    // val v0 = Array(8.0, 4.0, 0.0, 2.0, 16.0, 0.0, 13.0, 0.0)

    val l1 = List(ProjectionMetaData(List(0, 2),List(3, 9),List(1, 1),23), ProjectionMetaData(List(0, 1),List(3, 4),List(1, 1),26))
    val l2 = List(ProjectionMetaData(List(1),List(4),List(1),10), ProjectionMetaData(List(0, 2),List(3, 9),List(1, 1),23), ProjectionMetaData(List(),List(),List(),0), ProjectionMetaData(List(0, 1),List(3, 4),List(1, 1),26), ProjectionMetaData(List(0),List(3),List(1),9), ProjectionMetaData(List(2),List(9),List(1),5))

    def f(x: Double, y: Double) = Some(Interval(Some(x), Some(y)))

    val v1 = Array(new Payload(8.0, f(3.0, 5.0)), new Payload(6.0, f(2.0, 4.0)), new Payload(29.0, f(1.0, 8.0)), new Payload(0.0, None), new Payload(24.0, f(1.0, 6.0)), new Payload(4.0, f(4.0, 4.0)), new Payload(13.0, f(5.0, 8.0)), new Payload(2.0, f(2.0, 2.0)))
    val v2 = Array(new Payload(28.0, f(1.0, 6.0)), new Payload(15.0, f(2.0, 8.0)), new Payload(8.0, f(3.0, 5.0)), new Payload(6.0, f(2.0, 4.0)), new Payload(29.0, f(1.0, 8.0)), new Payload(0.0, None), new Payload(43.0, f(1.0, 8.0)), new Payload(24.0, f(1.0, 6.0)), new Payload(4.0, f(4.0, 4.0)), new Payload(13.0, f(5.0, 8.0)), new Payload(2.0, f(2.0, 2.0)), new Payload(37.0, f(1.0, 8.0)), new Payload(6.0, f(2.0, 4.0)), new Payload(14.0, f(2.0, 5.0)), new Payload(29.0, f(1.0, 8.0)))

    val b1 = SolverTools.mk_all_non_neg[Double](1 << 3)
    val b2 = SolverTools.mk_all_non_neg[Double](1 << 3)

    val s1 = Solver(3, b1, l1.map(_.accessible_bits), v1)
    val s2 = Solver(3, b2, l2.map(_.accessible_bits), v2)

    assert(s1.compute_bounds.toList == s2.compute_bounds.toList)
  }

/*
import backend._
import util._
import core._
val ab = util.Util.mkAB[Interval](4, _ => Interval(None, None))
val v = Array(new Payload(4, Some(Interval(None,     Some(5)))),
              new Payload(1, Some(Interval(Some(0),  Some(6)))),
              new Payload(1, Some(Interval(Some(1),  Some(2)))),
              new Payload(0, Some(Interval(Some(-3), Some(4)))))
val s = Solver(2, ab, List(List(0), List(1)), v)
*/
}



