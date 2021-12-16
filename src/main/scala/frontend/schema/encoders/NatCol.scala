package frontend.schema.encoders

import frontend.schema.{BitPosRegistry, RegisterIdx}
import util.BigBinary

import scala.io.Source
import scala.util.Try

/** A natural number-valued column.
 * The column bits represent the natural number directly, and
 * no map needs to be stored.
 * NatCol does not have a way or representing NULL values --
 * the default value is zero.
 */
@SerialVersionUID(8516410713123398680L)
class NatCol(max_value: Int = 0)(implicit bitPosRegistry: BitPosRegistry)  extends DynamicColEncoder[Int]  {
  register.registerIdx(max_value)
  override def encode_any(v: Any): BigBinary = v match {
    case i: Int => encode(i)
    case s: String if Try(s.toInt).isSuccess => encode(s.toInt)
    case _ => BigBinary(0)
  }
  override def queries(): Set[Seq[Int]] = (0 to bits.length).map(l => bits.take(l)).toSet //bits are stored in highest to lowest order
  def encode_locally(v: Int): Int = {
    assert(v >= 0)
    register.registerIdx(v); v
  }
  def decode_locally(i: Int) = i
}

//Encodes difference from min_value-1. Error or empty values are encoded as 0
class StaticNatCol(min_value: Int, max_value: Int, map_f : Any => Option[Int]) extends StaticColEncoder[Int] {
  override def encode_locally(v: Int): Int = (v-min_value+1)
  override def decode_locally(i: Int): Int = i + min_value - 1
  override def encode_any(v: Any): BigBinary = map_f(v).map(x => encode(x)).getOrElse(BigBinary(0))
  override def maxIdx: Int = if(max_value >= min_value) max_value - min_value + 1 else 0
  override def queries(): Set[Seq[Int]] = (0 to bits.length).map(l => bits.takeRight(l)).toSet  //storing bits in lowest to highest order
  override def n_bits: Int = if(max_value < min_value) 0 else math.ceil(math.log(max_value-min_value + 1)/math.log(2)).toInt
}
object StaticNatCol {
  def defaultToInt(v: Any) = v match {
    case i: Int => Some(i)
    case s: String => Try(s.toInt).toOption
  }
  def floatToInt(prec: Int)(v: Any) = {
    val mult = math.pow(10, prec)
    val dval = v match {
      case s: String => Try{s.toDouble}.toOption
      case d: Double => Some(d)
    }
    dval.map(d => math.round(d * mult))
  }
  def fromFile(filename: String, map_f: Any => Option[Int] = defaultToInt) = {
    val lines = Source.fromFile(filename).getLines().map(map_f).flatten.toSeq
    val min = lines.min
    val max = lines.max
    new StaticNatCol(min, max, map_f)
  }
}
class FixedPointCol(decimal: Int = 2, max_val: Double = 0.0)(implicit bitPosRegistry: BitPosRegistry) extends DynamicColEncoder[Double] {

  val multiplier = Math.pow(10, decimal)
  register.registerIdx((max_val* multiplier).toInt)

  override def queries(): Set[Seq[Int]] = (0 to bits.length).map(l => bits.take(l)).toSet

  override def encode_any(v: Any): BigBinary = v match {
    case i: Int => encode(i)
    case f: Float => encode(f)
    case d: Double => encode(d)
    case s: String if Try(s.toDouble).isSuccess => encode(s.toDouble)
  }

  def encode_locally(v: Double): Int = {
    val i = (v * multiplier).toInt
    register.registerIdx(i); i
  }

  def decode_locally(i: Int) = i/multiplier
}