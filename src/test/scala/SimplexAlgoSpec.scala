import core.solver.lpp._
import core.solver.{Rational, SolverTools, lpp}
import org.scalatest._
import IntervalTools._
import core.solver.RationalTools._
import core.solver.lpp.SparseMatrixImplicits._
import util.Util
class SimplexAlgoSpec extends FlatSpec with Matchers {


  "SimplexAlgo" should "not crash when there are no constraints" in {
    val qsize = 1
    val bounds = SolverTools.mk_all_non_neg[Rational](1 << qsize)
    val v = List[Rational]()
    val l = List[List[Int]]()
    val s = SparseSolver[Rational](qsize, bounds, l, v)
    s.propagate_bounds(0 to (1 << qsize) - 1)
    s.my_bounds(0 to (1 << qsize) - 1)
    val bou = s.bounds.toList
    val bou1 = List(Interval(Some(0),None), Interval(Some(0),None))
    assert(bou.toString == bou1.toString)
  }


  "SimplexAlgo" should "work when there are only base vars" in {
    val eqs : List[(List[(Int, Rational)], Rational)] =
      List((List((0, 1)), 5))

    val a = SimplexAlgo_Aux.mk_tableau(1, List(0), eqs)
    a.set_simple_objective(0, true)
    assert(a.algo.get.toInt == 5)
  }


  "SimplexAlgo test 1" should "work" in {

    val n_bits = 5
/*
    val fc     = CBackend.b.mk(n_bits,
                   StaticSchema.mk(n_bits).TupleGenerator(50, Sampling.f1))
    val m      = RandomizedMaterializationScheme(n_bits, .4, 2)
    val dc     = new DataCube(m, fc)
    var q      = Util.rnd_choose(n_bits, 3)
    val l      = m.prepare(q, 1, 1)
    val v      = dc.fetch(l).map(x => Rational(x.sm.toInt, 1))
    val l2     = l.map(_.accessible_bits)
*/
    val q      = List(1,2,4)
    val l2     = List(List(1), List(0), List(2))
    val v      = Array(100, 113, 106, 107, 126, 87).map(x => Rational(x, 1))
    val bounds = SolverTools.mk_all_non_neg[Rational](1 << q.length)
    val s      = lpp.SparseSolver[Rational](3, bounds, l2, v)

    s.my_bounds(0 to (1 << q.length) - 1)
    val r = s.bounds.toList.toString

    val r1 = List(Interval(Some(0),Some(100)),
                  Interval(Some(0),Some(100)),
                  Interval(Some(0),Some(106)),
                  Interval(Some(0),Some(107)),
                  Interval(Some(0),Some(87)),
                  Interval(Some(0),Some(87)),
                  Interval(Some(0),Some(87)),
                  Interval(Some(0),Some(87))
             ).map(IntervalTools.i2r(_)).toString

    assert(r == r1)

    val a = s.mk_tableau
    assert(a.M(4)(0) == Rational(2,1))
    a.set_simple_objective(0, true)
    a.algo
  }


  "handcrafted simplex instance 1" should "work" in {
    // base array: (d0, d1)[d2=0] = 5, 1, 10, 4
    //             (d0, d1)[d2=1] = 2, 6, 0,  3
    val s = SparseSolver[Double](3,
      Util.mkAB[Interval[Double]](8, _ => Interval(None, None)),
      List(List(0,1), List(1,2)),
      Vector(7,7,10,7, 6,14,8,3)
    )
    val a = s.mk_tableau
    a.set_simple_objective(0, true)

    val o = List((0,1.0), (8,-1000.0))
    val c = List(
      (List((0,1.0), (1, 1.0)          ),  6.0),
      (List((2,1.0), (3, 1.0)          ), 14.0),
      (List((0,1.0), (4, 1.0)          ),  7.0),
      (List((5,1.0), (0,-1.0)          ),  1.0),
      (List((2,1.0), (6, 1.0)          ), 10.0),
      (List((8,1.0), (2, 1.0), (7,-1.0)),  7.0)
    )
    val a2 = new SimplexAlgo(9, List(1,3,4,5,6,8), List(), c)
    a2.set_objective(o)
    a2.init_objective

    assert(a.M == a2.M)

    assert(a.algo == Some(6.0))
  }


  "handcrafted simplex instance 2" should "work" in {
  /*
  Z - 2x1 - x2                = 0
       x1      + x3           = 6
            x2      + x4      = 4
       x1 + x2           + x5 = 8
  */

  val objective   : List[(Int, Rational)] = List((0, 2), (1, 1))
  val constraints : List[(List[(Int, Rational)], Rational)] = List(
    (List((0, 1),         (2, 1)                ), 6),
    (List(        (1, 1),         (3, 1)        ), 4),
    (List((0, 1), (1, 1),                 (4, 1)), 8)
  )

  val a = new SimplexAlgo[Rational](5, List(2,3,4), List(), constraints)
  a.set_objective(objective)
  /* scala> a.tableau
     0     1     2    3    4    #
  Z  -2.0  -1.0  0.0  0.0  0.0  0.0
  2  1.0   0.0   1.0  0.0  0.0  6.0
  3  0.0   1.0   0.0  1.0  0.0  4.0
  4  1.0   1.0   0.0  0.0  1.0  8.0
  */

  a.M.pivot(1, 0)
  /* scala> a.tableau
     0    1     2     3    4    #
  Z  0.0  -1.0  2.0   0.0  0.0  12.0
  0  1.0  0.0   1.0   0.0  0.0  6.0
  3  0.0  1.0   0.0   1.0  0.0  4.0
  4  0.0  1.0   -1.0  0.0  1.0  2.0
  */

  a.M.pivot(3, 1)
  /* scala> a.tableau
     0    1    2     3    4     #
  Z  0.0  0.0  1.0   0.0  1.0   14.0
  0  1.0  0.0  1.0   0.0  0.0   6.0
  3  0.0  0.0  1.0   1.0  -1.0  2.0
  1  0.0  1.0  -1.0  0.0  1.0   2.0
  */

  assert(a.algo.get.toInt == 14)
  // Maximal value: 14 (v0 = 6, v1 = 2, v2 = 0, v3 = 2, v4 = 0)
  }


  "handcrafted simplex instance 3" should "work" in {
  val objective   : List[(Int, Rational)] = List((0, 2), (1, 3))
  val constraints : List[(List[(Int, Rational)], Rational)] = List(
    (List((0, -1), (1, 1), (2, 1)                                   ),  7),
    (List(         (1, 1),        (3, 1)                            ),  9),
    (List((0, 1),  (1, 2),               (4, 1)                     ), 22),
    (List((0, 1),  (1, 1),                      (5, 1)              ), 14),
    (List((0, 2),  (1, 1),                             (6, 1)       ), 22),
    (List((0, 1),                                             (7, 1)),  9)
  )
  val a2 = new SimplexAlgo[Rational](8, 2 to 7, List(), constraints)
  a2.set_objective(objective)

  a2.M.pivot(1,1)
  a2.M.pivot(2,0)
  a2.M.pivot(3,2)
  a2.M.pivot(4,3)

  //println(a2.tableau)
  /*
      0    1    2    3     4     5    6    7    #
  Z   0.0  0.0  0.0  0.0   1.0   1.0  0.0  0.0  36.0
  1   0.0  1.0  0.0  0.0   1.0  -1.0  0.0  0.0  8.0
  0   1.0  0.0  0.0  0.0  -1.0   2.0  0.0  0.0  6.0
  2   0.0  0.0  1.0  0.0  -2.0   3.0  0.0  0.0  5.0
  3   0.0  0.0  0.0  1.0  -1.0   1.0  0.0  0.0  1.0
  6   0.0  0.0  0.0  0.0   1.0  -3.0  1.0  0.0  2.0
  7   0.0  0.0  0.0  0.0   1.0  -2.0  0.0  1.0  3.0
  */

  assert(a2.algo.get.toInt == 36) // Optimal value: 36

  // (v0 = 6, v1 = 8, v2 = 5, v3 = 1, v4 = 0, v5 = 0, v6 = 2, v7 = 3)
  }


  "handcrafted simplex instance 4" should "work" in {
    val det_vars = Vector(1, 3, 4, 5, 6, 7)

    val eqs : List[(List[(Int, Rational)], Rational)] =
      List((List((0,1), (1,1)),6),
           (List((2,1), (3,1)),14),
           (List((0,1), (4,1)),7),
           (List((5,1), (0,-1)),1),
           (List((2,1), (6,1)),10),
           (List((7,1), (2,-1)),-7))

    val a = SimplexAlgo_Aux.mk_tableau(8, det_vars, eqs)
    a.set_simple_objective(0, true)
    a.M.pivot(6,2)
    a.M.pivot(1,0)
    assert(a.algo.get.toInt == 6)
  }


  "an objective on a basis variable" should "work without adding another slack variable" in {
    val constraints : List[(List[(Int, Rational)], Rational)] = List(
      (List((0, 1),         (2, 1)                ), 6),
      (List(        (1,-1),         (3, 1)        ), 4),
      (List((0, 1), (1, 1),                 (4, 1)), 8)
    )
    val objective   : List[(Int, Rational)] = List((3, 1))
    val a = new SimplexAlgo[Rational](5, List(2,3,4), List(), constraints)
    a.set_objective(objective)
    assert(a.algo.get.toInt == 12)
  }


  "Empty tableau" should "not crash" in {
    import core.solver.RationalTools._
    val qsize = 1
    val bounds = SolverTools.mk_all_non_neg[Rational](1 << qsize)
    val v = List[Rational]()
    val l = List[List[Int]]()
    val s = SparseSolver[Rational](qsize, bounds, l, v)
    s.propagate_bounds(0 to (1 << qsize) - 1)
    s.my_bounds(0 to (1 << qsize) - 1)
  }
}


