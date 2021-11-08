package frontend.schema.encoders

import frontend.schema.{BitPosRegistry, RegisterIdx}
import util.BigBinary

import java.util.Date
import scala.util.Try

class DateCol(referenceYear: Int = 0) extends ColEncoder[Date] with RegisterIdx {

  override def queries(): Set[List[Int]] =  {
    val ybits = yCol.bits
    val ymbits = mCol.bits ++ ybits

    val yQ = yCol.queries
    val mQ = mCol.queries.flatMap(q => Set(q, q ++ ybits))
    val dQ = dCol.queries.flatMap(q => Set(q, q ++ ymbits))
    yQ union mQ union dQ
  }
  val yCol = new NatCol
  val mCol = new NatCol
  val dCol = new NatCol


  override def setRegistry(r: BitPosRegistry): Unit = {
    yCol.setRegistry(r)
    mCol.setRegistry(r)
    dCol.setRegistry(r)
  }

  override def encode(v: Date): BigBinary = {
    val year = v.getYear
    val month = v.getMonth
    val day = v.getDate

    val yearDelta = year - (referenceYear-1900)
    assert(yearDelta >=  0)
    yCol.encode(yearDelta) + mCol.encode(month) + dCol.encode(day)
  }

  override def encode_locally(v: Date): Int = ???
  override def decode_locally(i: Int): Date = ???


  override def refreshBits: Unit = {
    bits = yCol.bits ++ mCol.bits ++ dCol.bits
  }

  override def decode(b: BigBinary): Date = {
    val yD = yCol.decode(b)
    val m = mCol.decode(b)
    val d = dCol.decode(b)
    new Date(yD + (referenceYear - 1900), m, d)
  }

  override def encode_any(v: Any): BigBinary = v match {
    case d: Date => encode(d)
    case s: String if Try(Date.parse(s)).isSuccess => encode(new Date(s))
  }
}
