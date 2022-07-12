import core.solver.lpp.Interval
import org.scalatest._

class IntervalSpec extends FlatSpec with Matchers {
  val i1 = Interval(None, Some(3.0))
  val i2 = Interval[Double](None, None)
  val i3 = Interval(Some(1.0), Some(4.0))
  val i4 = Interval(Some(2.0), None)
  val i5 = Interval(Some(-1.0), Some(4.0))

  def force_lb(i: Interval[Double]) = i.lb.getOrElse(-1.0/0)
  def force_ub(i: Interval[Double]) = i.ub.getOrElse( 1.0/0)

  "Sum of unbounded intervals" should "work" in {
     assert(List(i1, i2    ).map(x => force_ub(x)).sum == 1.0/0)
     assert(List(i1, i3    ).map(x => force_ub(x)).sum == 7.0)
     assert(List(i1, i3, i4).map(x => force_lb(x)).sum == -1.0/0)
     assert(List(i3, i4    ).map(x => force_lb(x)).sum == 3.0)
  }

  "Multiplication with a scalar" should "work" in {
    assert(i4 * -2 == Interval(None, Some(-4)))
    assert(i5 * -2 == Interval(Some(-8), Some(2)))
    assert(i5 *  3 == Interval(Some(-3), Some(12)))
  }

  "Intersection" should "work" in {
    assert(i1.intersect(i5) == Interval(Some(-1), Some(3)))
    assert(i3.intersect(i5) == Interval(Some(1), Some(4)))
  }
}



