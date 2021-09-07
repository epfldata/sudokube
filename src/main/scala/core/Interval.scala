//package ch.epfl.data.sudokube
package core


/** empty intervals cannot be represented.
*/
case class Interval[T](
  lb: Option[T],
  ub: Option[T]
)(implicit num: Numeric[T]) {

  protected def bravely(a: Option[T], b: Option[T], f: (T, T) => T) =
    (a, b) match {
    case (   None,    None) => None
    case (   None, Some(y)) => Some(y)
    case (Some(x),    None) => Some(x)
    case (Some(x), Some(y)) => Some(f(x,y))
  }

  protected def cautiously(a: Option[T], b: Option[T], f: (T, T) => T) =
    (a, b) match {
    case (Some(a), Some(b)) => Some(f(a, b))
    case (Some(a), None   ) => None
    case (None,    Some(b)) => None
    case (None,    None   ) => None
  }

  // test at interval construction time.
  cautiously(ub, lb, num.minus) match {
    case Some(x) if num.lt(x, num.zero) =>
      throw new IllegalStateException("Broken interval [" + lb + ", " + ub + "]")
    case _ => {}
  }

  /** multiplies the bounds of an interval with a constant.
    Example: {{{
      scala> Interval[Int](Some(3),Some(5)) * -2
      res1: core.Interval[Int] = Interval(Some(-10),Some(-6))
      }}}
  */
  def *(c: T) : Interval[T] = {
    def omul(x: Option[T], c: T) : Option[T] = x.map(num.times(_, c))

    if(num.gteq(c, num.zero)) Interval(omul(lb, c), omul(ub, c))
    else                      Interval(omul(ub, c), omul(lb, c))
  }

  /** sums up two intervals.
      Example: {{{
      scala> Interval[Int](Some(3),Some(5)) + Interval[Int](Some(3),Some(5))
      res2: core.Interval[Int] = Interval(Some(6),Some(10))
      scala> Interval[Int](Some(3),Some(5)) + Interval[Int](None,Some(5))
      res3: core.Interval[Int] = Interval(None,Some(10))
      }}}
  */
  def +(other: Interval[T]) : Interval[T] = {
    val new_lb = cautiously(lb, other.lb, num.plus)
    val new_ub = cautiously(ub, other.ub, num.plus)
    Interval(new_lb, new_ub)
  }

  /** Computes a minimally-sized interval that contains both this and other.
      Example: {{{
      scala> Interval[Int](Some(6),Some(7)).envelope(Interval[Int](None,Some(5)))
      res5: core.Interval[Int] = Interval(None,Some(7))
      }}}
  */
  def envelope(other: Interval[T]) : Interval[T] = {
    val min_lb = cautiously(lb, other.lb, num.min)
    val max_ub = cautiously(ub, other.ub, num.max)
    Interval(min_lb, max_ub)
  }

  /** intersects two intervals.

      Note: We do not have a way of representing
      an empty interval, so this method may throw an exception.

      Example: {{{
      scala> Interval[Int](Some(6),Some(7)).intersect(Interval[Int](None,Some(5)))
      java.lang.Exception: Broken interval [Some(6), Some(5)]

      scala> Interval[Int](Some(1),Some(2)).intersect(Interval[Int](None,Some(5)))
      res7: core.Interval[Int] = Interval(Some(1),Some(2))
      }}}
  */
  def intersect(other: Interval[T]) : Interval[T] = {
    val max_lb = bravely(lb, other.lb, num.max)
    val min_ub = bravely(ub, other.ub, num.min)
    Interval(max_lb, min_ub)
  }

  def isPoint: Boolean = (lb, ub) match {
    case (Some(x), Some(y)) if (x == y) => true
    case _ => false
  }

  /** computes the width of an interval. */
  def span: Option[T] = cautiously(ub, lb, num.minus)

  def format(format_v: T => String) : String = (lb, ub) match {
    case(Some(lb), Some(ub)) =>
      if(lb == ub) format_v(lb)
      else "[" + format_v(lb) + ", " + format_v(ub) + "]"
           // format_v((lb + ub)/2) + "+-" + format_v((ub - lb)/2)
    case(None, Some(ub)) => "]-i, " + format_v(ub) + "]"
    case(Some(lb), None) => "[" + format_v(lb) + ", i["
    case(None, None)     => "]-i, i["
  }
}


object IntervalTools {
  def d2i(di: Interval[Double]) : Interval[Int] = di match {
    case Interval(Some(lb), Some(ub)) =>
      Interval(Some(lb.round.toInt), Some(ub.round.toInt))
    case Interval(None, Some(ub)) => Interval(None, Some(ub.round.toInt))
    case Interval(Some(lb), None) => Interval(Some(lb.round.toInt), None)
    case Interval(None, None)     => Interval[Int](None, None)
  }

  def i2d(di: Interval[Int]) : Interval[Double] = di match {
    case Interval(Some(lb), Some(ub)) =>
      Interval(Some(lb.toDouble), Some(ub.toDouble))
    case Interval(None, Some(ub)) => Interval(None, Some(ub.toDouble))
    case Interval(Some(lb), None) => Interval(Some(lb.toDouble), None)
    case Interval(None, None)     => Interval[Double](None, None)
  }

  import RationalTools._

  def i2r(di: Interval[Int]) : Interval[Rational] = di match {
    case Interval(Some(lb), Some(ub)) =>
      Interval(Some(Rational(lb, 1)), Some(Rational(ub, 1)))
    case Interval(None, Some(ub)) => Interval(None, Some(Rational(ub, 1)))
    case Interval(Some(lb), None) => Interval(Some(Rational(lb, 1)), None)
    case Interval(None, None)     => Interval[Rational](None, None)
  }

  def point[T: Numeric](x: T) = Interval(Some(x), Some(x))

  def sum[T](l: Seq[Interval[T]])(implicit num: Numeric[T]) : Interval[T] =
    l.foldLeft(point(num.zero))(_+_)
}


