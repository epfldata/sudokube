package frontend.schema.encoders

import frontend.schema.{BitPosRegistry, RegisterIdx}
import util.BigBinary

import scala.util.matching.Regex

class PositionCol(reference: (Double, Double), precision: Int, maxVal: (Double, Double))(implicit bitPosRegistry: BitPosRegistry) extends ColEncoder[(Double, Double)]  {
  val long = new FixedPointCol(precision, maxVal._1)
  val lat = new FixedPointCol(precision, maxVal._2)

  override def encode(v: (Double, Double)): BigBinary = {
    val dlong = v._1 - reference._1
    val dlat = v._2 - reference._2
    assert(dlat >= 0)
    assert(dlong >= 0)
    long.encode(dlong) + lat.encode(dlat)
  }


  override def bits: Seq[Int] = long.bits ++ lat.bits

  override def bitsMin: Int = ???

  override def isRange: Boolean = ???

  override def maxIdx: Int = ???

  val regex = ("POINT \\((-?\\d*.\\d*) (-?\\d*.\\d*)\\)")

  override def encode_any(v: Any): BigBinary = v match {
    case s: String if s.matches(regex) =>
      val arr = s.drop(7).dropRight(1).split(" ").map(_.toDouble)
      encode((arr(0), arr(1)))
    case dd: (Double, Double) => encode(dd)
    case s: String if s.isEmpty => BigBinary(0)

  }

  override def encode_locally(v: (Double, Double)): Int = ???

  override def decode_locally(i: Int): (Double, Double) = ???

  override def queries(): Set[Seq[Int]] = lat.queries.flatMap(q1 => long.queries.map(q2 => q1 ++ q2))
}
