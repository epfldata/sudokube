import org.scalatest._
import core._
import core.solver.RationalTools._
import core.solver.moment.Strategy._
import core.solver._
import core.solver.moment.{CoMoment4Solver, Moment1Transformer, MomentSolverAll}
import util.BitUtils

class MomentSolverAllSpec extends FlatSpec with Matchers {
  implicit def listToInt = BitUtils.SetToInt(_)
  "MomentSolver " should " work when full cuboid is known using fast solve " in {
    val cuboid = Array(1, 3, 2, 1, 5, 1, 0, 2).map(_.toDouble)
    val solver = new MomentSolverAll[Double](3)
    solver.add(List(0, 1, 2), cuboid)
    solver.fillMissing()
    solver.fastSolve()
    assert(cuboid.sameElements(solver.solution))
  }

  "Moment Solver CoMoment3 strategy" should "extrapolate correctly" in {
    val solver = new MomentSolverAll[Rational](3, CoMoment3)
    val actual = Array(0, 1, 3, 1, 7, 2, 3, 0).map(_.toDouble)
    solver.add(List(2), Array(5, 12).map(Rational(_, 1)))
    solver.add(List(0, 1), Array(7, 3, 6, 1).map(Rational(_, 1)))
    solver.add(List(1, 2), Array(1, 4, 9, 3))
    solver.add(List(0, 2), Array(3, 2, 10, 2).map(Rational(_, 1)))
    solver.fillMissing()

    def r(i: Int) = Rational(i, 1)

    val moments = List[Rational](r(17), r(4), r(7), r(1), r(12), r(2), r(3), Rational(-26, 289))
    assert(moments sameElements solver.sumValues)
  }

  "Moment Solver CoMoment4 strategy" should "extrapolate correctly" in {
    val solver = new MomentSolverAll[Rational](3, CoMoment4)
    val actual = Array(0, 1, 3, 1, 7, 2, 3, 0).map(_.toDouble)
    solver.add(List(2), Array(5, 12).map(Rational(_, 1)))
    solver.add(List(0, 1), Array(7, 3, 6, 1).map(Rational(_, 1)))
    solver.add(List(1, 2), Array(1, 4, 9, 3))
    solver.add(List(0, 2), Array(3, 2, 10, 2).map(Rational(_, 1)))
    solver.fillMissing()

    def r(i: Int) = Rational(i, 1)

    val moments = List[Rational](r(17), r(4), r(7), r(1), r(12), r(2), r(3), Rational(-26, 289))
    assert(moments sameElements solver.sumValues)
  }

  "CoMoment4 Solver batch mode" should "extrapolate correctly" in {
    import frontend.experiments.Tools.round
    val primaryMoments = List(0 -> 17.0, 1 -> 4.0, 2 -> 7.0, 4 -> 12.0)
    val solver = new CoMoment4Solver[Double](3, true, Moment1Transformer(), primaryMoments)
    val actual = Array(0, 1, 3, 1, 7, 2, 3, 0).map(_.toDouble)
    solver.add(List(2), Array(5, 12).map(_.toDouble))
    solver.add(List(0, 1), Array(7, 3, 6, 1).map(_.toDouble))
    solver.add(List(1, 2), Array(1, 4, 9, 3).map(_.toDouble))
    solver.add(List(0, 2), Array(3, 2, 10, 2).map(_.toDouble))
    solver.fillMissing()
    val moments = List[Double](17.0, 4.0, 7.0, 1.0, 12.0, 2.0, 3.0, -26.0 / 289).map(round(_, 6))

    val smoments = solver.moments.map(round(_, 6))
    (moments zip smoments).filter(x => x._1 != x._2).foreach(println)
    assert(moments sameElements smoments)
  }

  "CoMoment4 Solver online mode" should "extrapolate correctly" in {
    import frontend.experiments.Tools.round
    val primaryMoments = List(0 -> 17.0, 1 -> 4.0, 2 -> 7.0, 4 -> 12.0)
    val solver = new CoMoment4Solver[Double](3, false, Moment1Transformer(), primaryMoments)
    val actual = Array(0, 1, 3, 1, 7, 2, 3, 0).map(_.toDouble)
    solver.add(List(2), Array(5, 12).map(_.toDouble))
    solver.add(List(0, 1), Array(7, 3, 6, 1).map(_.toDouble))
    solver.add(List(1, 2), Array(1, 4, 9, 3).map(_.toDouble))
    solver.add(List(0, 2), Array(3, 2, 10, 2).map(_.toDouble))
    solver.fillMissing()
    val moments = List[Double](17.0, 4.0, 7.0, 1.0, 12.0, 2.0, 3.0, -26.0 / 289).map(round(_, 6))

    val smoments = solver.moments.map(round(_, 6))
    (moments zip smoments).filter(x => x._1 != x._2).foreach(println)
    assert(moments sameElements smoments)
  }
}
