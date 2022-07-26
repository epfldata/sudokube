package frontend.generators

import frontend.Sampling
import util.BigBinary

import java.io.PrintStream

trait ValueGenerator {
  lazy val out = new PrintStream("basecube.txt")
  def apply(k: BigBinary): Long
  def applyWithLog(k: BigBinary): Long = {
    val v = apply(k)
    if(v != 0) out.println(k + " -> " + v + " :: " + v.toBinaryString)
    v
  }
}

case class ConstantValueGenerator(n: Long) extends ValueGenerator {
  override def apply(k: BigBinary): Long = n
}

case class RandomValueGenerator(n: Long) extends ValueGenerator {
  val r = new scala.util.Random()
  override def apply(k: BigBinary): Long = (r.nextDouble()* n + 0.5).toLong
}

/**
 *
 * @param xcols : Columns on which trend is correlated
 * @param zcols : Columns on which filter is applied before trend is generated
 * @param zval : Filer value (between 0 and 1 << zcols.length)
 * @param yslope : Slope of trend between -1 and 1
 * @param yscale : scale factor for trend
 */
case class TrendValueGenerator(xcols : List[Int], zcols: List[Int], zval: Int, yslope: Double, yscale: Double) extends ValueGenerator {
  override def apply(k: BigBinary): Long = {

    val z = k.valueOf(zcols)
    val x = k.valueOf(xcols)
    if(z == zval)
      Sampling.t1(x+1, yslope, yscale)/(1 << xcols.length)
    else
      0L
  }
}
case class SinValueGenerator(xcols : List[Int], zcols: List[Int], zval: Int, yslope: Double, yscale: Double) extends ValueGenerator {
  override def apply(k: BigBinary): Long = {
    val z = k.valueOf(zcols)
    val x = k.valueOf(xcols)
    if(z == zval)
      (yscale * yslope * (1 - Math.cos(Math.PI * 2 * x / (1 << xcols.length)))).toLong
    else
      0L
  }
}

case class ValueMapper(vg: ValueGenerator, f: Long => Long) extends ValueGenerator {
  override def apply(k: BigBinary): Long = f(vg(k))
}

/**
 * Produces linear sum of individual value generators
 */
case class SumValueGenerator(vs: Seq[ValueGenerator]) extends ValueGenerator {
  override def apply(k: BigBinary): Long = vs.map(_.apply(k)).sum
}

