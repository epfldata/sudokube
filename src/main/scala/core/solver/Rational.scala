package core.solver

sealed class Rational(_a: BigInt, _b: BigInt) extends Ordered[Rational] {
  assert(_b != 0)

  private val g = _a.gcd(_b)

  val a = (if(_b < 0) -1 else 1) * _a / g
  val b = (_b / g).abs

  def +(that: Rational): Rational = {
    val g = b.gcd(that.b)
    Rational((a * (that.b / g)) + (that.a * (b / g)), b * (that.b / g))
  }

  def -(that: Rational): Rational = this + Rational(-that.a, that.b)
  def *(that: Rational): Rational = Rational(a * that.a, b * that.b)
  def /(that: Rational): Rational = Rational(a * that.b, b * that.a)

  //def *(that: Rational): Rational = {
  //  val g1 = gcd(that.a, b)
  //  val g2 = gcd(a, that.b)
  //  Rational((a/g2) * (that.a/g1), (b/g1) * (that.b/g2))
  //}
  //def /(that: Rational): Rational = {
  //  val g1 = gcd(that.b, b)
  //  val g2 = gcd(a, that.a)
  //  Rational((a/g2) * (that.b/g1), (b/g1) * (that.a/g2))
  //}
  def negate: Rational = Rational(-a, b)

  def compare(that: Rational): Int =
   ((this.a * that.b) - (that.a * this.b)).signum

  override def equals(that: Any) = {
    if(that.isInstanceOf[Rational]) {
      val other = that.asInstanceOf[Rational]
      (a == other.a) && (b == other.b)
    }
    else false
  }

  override def toString = if(b == 1) a.toString else a + "/" + b
  def toDouble = a.toDouble / b.toDouble
  def toFloat  = a.toFloat / b.toFloat
  def toInt    = (a / b).toInt
  def toLong   = (a / b).toLong
}


object Rational {
  def apply(a: BigInt, b: BigInt) = new Rational(a, b)
  def abs(x: BigInt) = if(x > 0) x else -x
}


object RationalTools {

  implicit object RationalOps extends Fractional[Rational] {
    override def zero = Rational(0, 1)

    def    plus(x: Rational, y: Rational) = x + y
    def   minus(x: Rational, y: Rational) = x - y
    def   times(x: Rational, y: Rational) = x * y
    def     div(x: Rational, y: Rational) = x / y
    def compare(x: Rational, y: Rational) = x compare y

    def  fromInt(x: Int)      = Rational(x, 1)
    def toDouble(x: Rational) = x.toDouble
    def  toFloat(x: Rational) = x.toFloat
    def    toInt(x: Rational) = x.toInt
    def   toLong(x: Rational) = x.toLong
    def   negate(x: Rational) = x.negate
  }

  implicit def Rat2Int(x: Int) = Rational(x, 1)
}

