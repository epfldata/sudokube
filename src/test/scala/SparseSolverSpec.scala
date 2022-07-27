import core.solver.RationalTools
import core.solver.lpp.{Interval, IntervalTools, SparseSolver}
import core.solver.{Rational, SolverTools, lpp}
import org.scalatest._
import planning.{NewProjectionMetaData, ProjectionMetaData}
import util.BitUtils


class SparseSolverSpec extends FlatSpec with Matchers {
  implicit def listToInt = BitUtils.SetToInt(_)
  implicit def listOfListToListOfList(l: List[List[Int]]) = l.map(listToInt)

  // the solution is: Array(1,7,3,0,10,2,4,20)
  val l1 = List(List(0,1), List(1,2))
  val v1 = Array(11,9,7,20,8,3,12,24)

  val l2 = List(List(2), List(1), List(0), List())
  val v2 = Array(10, 26, 14, 22, 16, 20, 36)

  val r1 = List(Interval(Some(0),Some(8)), Interval(Some(0),Some(8)), Interval(Some(0),Some(3)), Interval(Some(0),Some(3)), Interval(Some(3),Some(11)), Interval(Some(1),Some(9)), Interval(Some(4),Some(7)), Interval(Some(17),Some(20)))

  val r2 = List(Interval(Some(1),Some(4)), Interval(Some(4),Some(7)), Interval(Some(0),Some(3)), Interval(Some(0),Some(3)), Interval(Some(7),Some(10)), Interval(Some(2),Some(5)), Interval(Some(4),Some(7)), Interval(Some(17),Some(20)))

  val r3 = List(Interval(Some(1),Some(1)), Interval(Some(7),Some(7)), Interval(Some(3),Some(3)), Interval(Some(0),Some(0)), Interval(Some(10),Some(10)), Interval(Some(2),Some(2)), Interval(Some(4),Some(4)), Interval(Some(20),Some(20)))

    val r4 = List(Interval(Some(0),Some(0)), Interval(Some(0),Some(10)), Interval(Some(0),Some(10)), Interval(Some(0),Some(10)), Interval(Some(0),Some(14)), Interval(Some(0),Some(14)), Interval(Some(0),Some(16)), Interval(Some(0),Some(30)))

    val r5 = List(Interval(Some(0),Some(0)), Interval(Some(0),Some(10)), Interval(Some(0),Some(10)), Interval(Some(0),Some(10)), Interval(Some(0),Some(14)), Interval(Some(0),Some(14)), Interval(Some(0),Some(16)), Interval(Some(0),Some(16)))


  "SparseSolver[Int] test 1" should "work" in {
    val b = SolverTools.mk_all_non_neg[Int](1 << 3)
    import util.SloppyFractionalInt._
    val s = SparseSolver[Int](3, b, l1, v1)
    s.propagate_bounds(0 to 7)
    assert(s.bounds.toList == r1)
    assert(s.df == 2)

    s.add(List((List(0,2), 4)))
    s.gauss(List(2))
    s.propagate_bounds(0 to 7)
    assert(s.bounds.toList == r2)
    assert(s.df == 1)

    s.add(List((List(0), 1)))
    s.gauss(List(0))
    s.propagate_bounds(0 to 7)
    assert(s.bounds.toList == r3)
    assert(s.df == 0)
  }

  "SparseSolver[Rational] test 1" should "work" in {
    import RationalTools._
    val vr = v1.map(Rational(_, 1))
    val b = SolverTools.mk_all_non_neg[Rational](1 << 3)
    val s = SparseSolver[Rational](3, b, l1, vr)
    s.propagate_bounds(0 to 7)
    assert(s.bounds.toList == r1.map(IntervalTools.i2r(_)))
    assert(s.df == 2)

    s.add(List((List(0,2), Rational(4,1))))
    s.gauss(List(2))
    s.propagate_bounds(0 to 7)
    assert(s.bounds.toList == r2.map(IntervalTools.i2r(_)))
    assert(s.df == 1)

    s.add(List((List(0), Rational(1,1))))
    s.gauss(List(0))
    s.propagate_bounds(0 to 7)
    assert(s.bounds.toList == r3.map(IntervalTools.i2r(_)))
    assert(s.df == 0)
  }

  "SparseSolver[Double] test 1" should "work" in {
    val b = SolverTools.mk_all_non_neg[Double](1 << 3)
    val vd = v1.map(_.toDouble)
    val s = SparseSolver[Double](3, b, l1, vd)
    s.propagate_bounds(0 to 7)
    assert(s.bounds.toList == r1.map(IntervalTools.i2d(_)))

    s.simplex_add
    s.propagate_bounds(0 to 7)
    assert(s.bounds.toList == r1.map(IntervalTools.i2d(_)))

    s.add(List((List(0,2), 4)))
    s.gauss(List(2))
    s.propagate_bounds(0 to 7)
    assert(s.bounds.toList == r2.map(IntervalTools.i2d(_)))

    s.add(List((List(0), 1)))
    s.gauss(List(0))
    s.propagate_bounds(0 to 7)
    assert(s.bounds.toList == r3.map(IntervalTools.i2d(_)))
  }

  "SparseSolver[Int] test 2" should "work" in {
    val bounds = SolverTools.mk_all_non_neg[Int](1 << 3)
    import util.SloppyFractionalInt._
    val s = SparseSolver[Int](3, bounds, l2, v2)
    s.simplex_add
    s.propagate_bounds(0 to 7)

    assert(s.bounds.toList == r4)

    s.compute_bounds

    assert(s.bounds.toList == r4)
  }

  "SparseSolver[Double] test 2" should "work" in {

    val vd = v2.map(_.toDouble)
    val bounds = SolverTools.mk_all_non_neg[Double](1 << 3)
    val s = SparseSolver[Double](3, bounds, l2, vd)
    s.simplex_add
    s.propagate_bounds(0 to 7)

    val r6 = List(Interval(Some(0.0),Some(10.0)), Interval(Some(0.0),Some(10.0)), Interval(Some(0.0),Some(10.0)), Interval(Some(0.0),Some(10.0)), Interval(Some(0.0),Some(14.0)), Interval(Some(0.0),Some(14.0)), Interval(Some(0.0),Some(16.0)), Interval(Some(0.0),Some(50.0)))

    assert(s.bounds.toList == r6)

    assert(r6 != r4.map(IntervalTools.i2d(_)))
    // the integer version is actually buggy

    // IMPROVEMENT
    s.compute_bounds

    assert(s.bounds.toList == List(Interval(Some(0.0),Some(10.0)), Interval(Some(0.0),Some(10.0)), Interval(Some(0.0),Some(10.0)), Interval(Some(0.0),Some(10.0)), Interval(Some(0.0),Some(14.0)), Interval(Some(0.0),Some(14.0)), Interval(Some(0.0),Some(16.0)), Interval(Some(0.0),Some(20.0))))


    /* Note: This one has a -2 weight in the matrix!
    s.M
    res0: SparseMatrix[Double] =
    1.0   1.0   1.0   1.0  0.0   0.0  0.0  0.0  10.0
    1.0   1.0   0.0   0.0  1.0   1.0  0.0  0.0  14.0
    1.0   0.0   1.0   0.0  1.0   0.0  1.0  0.0  16.0
    -2.0  -1.0  -1.0  0.0  -1.0  0.0  0.0  1.0  -4.0
    */
  }

  "SparseSolver[Double] test 3" should "work" in {
    // correct query result:
    // val v0 = Array(8.0, 4.0, 0.0, 2.0, 16.0, 0.0, 13.0, 0.0)

    //WARNING: Implicitly encoding List[Int] using Int
    val l1 = List(NewProjectionMetaData(List(0, 2), 23, 2, Vector(0, 1)), NewProjectionMetaData(List(0, 1), 26, 2, Vector(0, 1)))
    val l2 = List(NewProjectionMetaData(List(1), 10, 1, Vector(0)), NewProjectionMetaData(List(0, 2), 23, 2, Vector(0, 1)), NewProjectionMetaData(List(), 0, 0,Vector()), NewProjectionMetaData(List(0, 1),26, 2, Vector(0, 1)), NewProjectionMetaData(List(0), 9, 1, Vector(0)), NewProjectionMetaData(List(2),5, 1, Vector(0)))

    def f(x: Double, y: Double) = Some(Interval(Some(x), Some(y)))

    val v1 = Array(8, 6, 29, 0, 24, 4, 13, 2).map(_.toDouble)
    val v2 = Array(28, 15, 8, 6, 29, 0, 43, 24, 4, 13, 2, 37, 6, 14, 29).map(_.toDouble)

    val b1 = SolverTools.mk_all_non_neg[Double](1 << 3)
    val b2 = SolverTools.mk_all_non_neg[Double](1 << 3)

    val s1 = SparseSolver[Double](3, b1, l1.map(_.queryIntersection), v1)
    val s2 = SparseSolver[Double](3, b2, l2.map(_.queryIntersection), v2)

    s1.compute_bounds
    s2.compute_bounds
    assert(s1.bounds.toList == s2.bounds.toList)
  }

  "Simplex on the free variables" should "not be sufficient" in {

    // the second scenario is an extension of the first.
    // nevertheless, the bounds are narrower in the first scenario.
    // this is an example demonstrating that it is not sufficient to only
    // run the simplex algorithm on the free variables and propagate.

    import RationalTools._

    val l1 = List(List(1))
    val v1 = Array(229, 229).map(Rational(_, 1))
    val l2 = List(List(1), List(0), List(2))
    val v2 = Array(229, 229, 182, 276, 241, 217).map(Rational(_, 1))

    val bounds1 = SolverTools.mk_all_non_neg[Rational](1 << 3)
    val bounds2 = SolverTools.mk_all_non_neg[Rational](1 << 3)

    val s1 = SparseSolver[Rational](3, bounds1, l1, v1)
    val s2 = SparseSolver[Rational](3, bounds2, l2, v2)
    s1.propagate_bounds(0 to 7)
    s2.propagate_bounds(0 to 7)
    assert(s1.cumulative_interval_span.get.toInt == 1832)
    assert(s2.cumulative_interval_span.get.toInt == 2190)

    val free_vars1 = List(0, 1, 2, 3, 4, 6)
    s1.my_bounds(free_vars1)
    val free_vars2 = List(0, 1, 2, 4)
    s2.my_bounds(free_vars2)

    assert(s1.cumulative_interval_span.get.toInt == 1832)
    assert(s2.cumulative_interval_span.get.toInt == 2190)

    s1.compute_bounds
    s2.compute_bounds
    assert(s1.cumulative_interval_span.get.toInt == 1832)
    assert(s2.cumulative_interval_span.get.toInt == 1620)
  }


/*
import backend._
import util._
import core._
val ab = util.Util.mkAB[Interval](4, () => Interval(None, None))
val v = Array(new Payload(4, Some(Interval(None,     Some(5)))),
              new Payload(1, Some(Interval(Some(0),  Some(6)))),
              new Payload(1, Some(Interval(Some(1),  Some(2)))),
              new Payload(0, Some(Interval(Some(-3), Some(4)))))
val s = Solver(2, ab, List(List(0), List(1)), v)
*/
}



