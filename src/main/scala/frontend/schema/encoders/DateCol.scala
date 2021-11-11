package frontend.schema.encoders

import frontend.schema.{BitPosRegistry, RegisterIdx}
import util.BigBinary

import java.text.SimpleDateFormat
import java.util.Date
import scala.util.Try

class DateCol(referenceYear: Int = 0) extends ColEncoder[Date] with RegisterIdx {

  override def queries(): Set[List[Int]] = {
    val ybits = yCol.bits
    val ymbits = mCol.bits ++ ybits

    val yQ = yCol.queries
    val mQ = mCol.queries.flatMap(q => Set(q, q ++ ybits))
    val dQ = dCol.queries.flatMap(q => Set(q, q ++ ymbits))
    val hrQ = hrCol.queries
    yQ union mQ union dQ union hrQ
  }

  val yCol = new NatCol
  val mCol = new NatCol
  val dCol = new NatCol
  val hrCol = new NatCol
  val minCol = new NatCol
  val secCol = new NatCol


  override def setRegistry(r: BitPosRegistry): Unit = {
    yCol.setRegistry(r)
    mCol.setRegistry(r)
    dCol.setRegistry(r)
    hrCol.setRegistry(r)
    minCol.setRegistry(r)
    secCol.setRegistry(r)
  }

  override def encode(v: Date): BigBinary = {
    val year = v.getYear
    val month = v.getMonth
    val day = v.getDate
    val hour = v.getHours
    val min = v.getMinutes
    val sec = v.getSeconds

    val yearDelta = year - (referenceYear - 1900)
    assert(yearDelta >= 0)
    yCol.encode(yearDelta) + mCol.encode(month) + dCol.encode(day) + hrCol.encode(hour) + minCol.encode(min) + secCol.encode(sec)
  }

  override def encode_locally(v: Date): Int = ???

  override def decode_locally(i: Int): Date = ???


  override def refreshBits: Unit = {
    println("date  = " + yCol.bits.length + " " + mCol.bits.length + " " + dCol.bits.length + " " + hrCol.bits.length + " " + minCol.bits.length + " " + secCol.bits.length)
    bits = yCol.bits ++ mCol.bits ++ dCol.bits ++ hrCol.bits ++ minCol.bits ++ secCol.bits
  }

  override def decode(b: BigBinary): Date = {
    val yD = yCol.decode(b)
    val m = mCol.decode(b)
    val d = dCol.decode(b)
    val hrs = hrCol.decode(b)
    val min = minCol.decode(b)
    val sec = secCol.decode(b)
    new Date(yD + (referenceYear - 1900), m, d, hrs, min, sec)
  }

  val f1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  override def encode_any(v: Any): BigBinary = v match {
    case d: Date => encode(d)
    case s: String if Try(Date.parse(s)).isSuccess => encode(new Date(s))
    case s: String if Try(f1.parse(s)).isSuccess => encode(f1.parse(s))
  }
}
