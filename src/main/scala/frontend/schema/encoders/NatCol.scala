package frontend.schema.encoders

import frontend.schema.{BitPosRegistry, RegisterIdx}
import util.BigBinary

import scala.util.Try

/** A natural number-valued column.
 * The column bits represent the natural number directly, and
 * no map needs to be stored.
 * NatCol does not have a way or representing NULL values --
 * the default value is zero.
 */
class NatCol() extends ColEncoder[Int] with RegisterIdx {
  override def encode_any(v: Any): BigBinary = v match {
    case i: Int => encode(i)
    case s: String if Try(s.toInt).isSuccess => encode(s.toInt)
  }


  override def queries(): Set[List[Int]] = (0 to bits.length).map(l => bits.take(l)).toSet

  def encode_locally(v: Int): Int = {
    assert(v >= 0)
    registerIdx(v); v
  }

  def decode_locally(i: Int) = i
}

class FixedPointCol(decimal: Int = 2) extends ColEncoder[Double] with RegisterIdx {
  val multiplier = Math.pow(10, decimal)

  override def queries(): Set[List[Int]] = (0 to bits.length).map(l => bits.take(l)).toSet

  override def encode_any(v: Any): BigBinary = v match {
    case i: Int => encode(i)
    case f: Float => encode(f)
    case d: Double => encode(d)
    case s: String if Try(s.toDouble).isSuccess => encode(s.toDouble)
  }

  def encode_locally(v: Double): Int = {
    val i = (v * multiplier).toInt
    registerIdx(i); i
  }

  def decode_locally(i: Int) = i/multiplier
}