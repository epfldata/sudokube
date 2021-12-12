package frontend.schema.encoders

import frontend.schema.{BitPosRegistry, RegisterIdx}
import util.BigBinary

import java.text.SimpleDateFormat
import java.util.Date
import scala.util.Try

@SerialVersionUID(2060625167200613195L) //for SSB
//@SerialVersionUID(-2090955600471017765L) //for NYC
class DateCol(referenceYear: Int, maxYear: Int, allocateMonth: Boolean = false, allocateDay: Boolean = false, allocateHr: Boolean = false, allocateMin: Boolean = false, allocateSec: Boolean = false)(implicit bitPosRegistry: BitPosRegistry) extends ColEncoder[Date]  {
//TODO: Split month into quarters


  val yCol = new NatCol(maxYear-referenceYear)
  val mCol = new NatCol(if(allocateMonth) 12 else 0)
  val dCol = new NatCol(if(allocateDay) 31 else 0)
  val hrCol = new NatCol(if(allocateHr) 23 else 0)
  val minCol = new NatCol(if(allocateMin) 60 else 0)
  val secCol = new NatCol(if(allocateSec) 60 else 0)

  override def bits: Seq[Int] = yCol.bits ++ mCol.bits ++ dCol.bits ++ hrCol.bits ++ minCol.bits ++ secCol.bits

  override def bitsMin: Int = ???

  override def isRange: Boolean = yCol.isRange && mCol.isRange && dCol.isRange && hrCol.isRange && minCol.isRange && secCol.isRange

  override def maxIdx: Int = ???

  override def queries(): Set[Seq[Int]] = {
    val ybits = yCol.bits
    val ymbits = mCol.bits ++ ybits

    val yQ = yCol.queries
    val mQ = mCol.queries.flatMap(q => Set(q, q ++ ybits))
    val dQ = dCol.queries.flatMap(q => Set(q, q ++ ymbits))
    val hrQ = hrCol.queries
    yQ union mQ union dQ union hrQ
  }

  override def encode(v: Date): BigBinary = {
    val year = v.getYear
    val month = v.getMonth
    val day = v.getDate
    val hour = v.getHours
    val min = v.getMinutes
    val sec = v.getSeconds

    if(year + 1900 < referenceYear || year + 1900 > maxYear)
      println(s"Overflow year range cur=${year+1900} min=${referenceYear} max=${maxYear} bitsMin=${yCol.bitsMin}")
    val yearDelta = year - (referenceYear - 1900)
    assert(yearDelta >= 0)
    yCol.encode(yearDelta) + mCol.encode(month) + dCol.encode(day) + hrCol.encode(hour) + minCol.encode(min) + secCol.encode(sec)
  }

  override def encode_locally(v: Date): Int = ???

  override def decode_locally(i: Int): Date = ???



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
  val f2 = new SimpleDateFormat("yyyyMMdd")

  override def encode_any(v: Any): BigBinary = v match {
    //case s: String if Try(f2.parse(s)).isSuccess => encode(f2.parse(s))
    case d: Date => encode(d)
    //case s: String if Try(Date.parse(s)).isSuccess => encode(new Date(s))
    //case s: String if Try(f1.parse(s)).isSuccess => encode(f1.parse(s))
  }
}
