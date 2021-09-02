package frontend.generators

import frontend.Sampling
import util.BigBinary

import java.io.PrintStream

trait ValueGenerator {
  val out = new PrintStream("basecube.txt")
  def apply(k: BigBinary): Int
  def applyWithLog(k: BigBinary): Int = {
    val v = apply(k)
    if(v != 0) out.println(k + " -> " + v + " :: " + v.toBinaryString)
    v
  }
}

case class ConstantValueGenerator(n: Int) extends ValueGenerator {
  override def apply(k: BigBinary): Int = n
}

case class RandomValueGenerator(n: Int) extends ValueGenerator {
  val r = new scala.util.Random()
  override def apply(k: BigBinary): Int = r.nextInt(n+1)
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
  override def apply(k: BigBinary): Int = {
    val z = k.valueOf(zcols)
    val x = k.valueOf(xcols)
    if(z == zval)
      Sampling.t1(x+1, yslope, yscale)/(1 << xcols.length)
    else
      0
  }
}
case class SinValueGenerator(xcols : List[Int], zcols: List[Int], zval: Int, yslope: Double, yscale: Double) extends ValueGenerator {
  override def apply(k: BigBinary): Int = {
    val z = k.valueOf(zcols)
    val x = k.valueOf(xcols)
    if(z == zval)
      (yscale * yslope * (1 - Math.cos(Math.PI * 2 * x / (1 << xcols.length)))).toInt
    else
      0
  }
}

/**
 * Produces linear sum of individual value generators
 */
case class SumValueGenerator(vs: Seq[ValueGenerator]) extends ValueGenerator {
  override def apply(k: BigBinary): Int = vs.map(_.apply(k)).sum
}

