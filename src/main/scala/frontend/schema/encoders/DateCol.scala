package frontend.schema.encoders

import frontend.schema.{BitPosRegistry, RegisterIdx}
import util.BigBinary

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.{Random, Try}

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


  override def bits: IndexedSeq[Int] = yCol.bits ++ mCol.bits ++ dCol.bits ++ hrCol.bits ++ minCol.bits ++ secCol.bits

  override def bitsMin: Int = ???

  override def isRange: Boolean = yCol.isRange && mCol.isRange && dCol.isRange && hrCol.isRange && minCol.isRange && secCol.isRange

  override def maxIdx: Int = ???

  override def queries(): Set[IndexedSeq[Int]] = {
    val ybits = yCol.bits
    val ymbits = mCol.bits ++ ybits
    val ymdbits = dCol.bits ++ ymbits
    val hrbits = hrCol.bits ++ ymdbits
    val hrminBits = minCol.bits ++ hrbits

    val yQ = yCol.queries
    val mQ = mCol.queries.flatMap(q => Set(q, q ++ ybits))
    val dQ = dCol.queries.flatMap(q => Set(q, q ++ ymbits))
    val hrQ = hrCol.queries.flatMap(q => Set(q, q ++ ymdbits))
    val minQ = minCol.queries.flatMap(q => Set(q, q ++ hrbits))
    val secQ = secCol.queries.flatMap(q => Set(q, q ++ hrminBits))
    yQ union mQ union dQ union hrQ union minQ union secQ
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
@SerialVersionUID(2557280452500488239L)
class StaticDateCol(map_f: Any => Option[Date], minYear: Int, maxYear: Int,  allocateMonth: Boolean = false, allocateDay: Boolean = false, allocateHr: Boolean = false, allocateMin: Boolean = false, allocateSec: Boolean = false) extends StaticColEncoder[Date] {
  val yearCol = new StaticNatCol(minYear, maxYear, _.asInstanceOf[Option[Date]].map(_.getYear))
  val quarterCol = new StaticNatCol(0, 3, _.asInstanceOf[Option[Date]].map(_.getMonth/4), false) //we want exactly 2 bits for quarter
  val monthCol = new StaticNatCol(0, 11, _.asInstanceOf[Option[Date]].map(_.getMonth))
  val dayCol = new StaticNatCol(1, 31, _.asInstanceOf[Option[Date]].map(_.getDate))
  //val dayOfWeekCol = new StaticNatCol(0, 6, _.asInstanceOf[Option[Date]].map(_.getDay))
  val hourCol = new StaticNatCol(0, 23 ,_.asInstanceOf[Option[Date]].map(_.getHours))
  val minuteCol = new StaticNatCol(0, 59, _.asInstanceOf[Option[Date]].map(_.getMinutes))
  val secondsCol = new StaticNatCol(0, 59, _.asInstanceOf[Option[Date]].map(_.getSeconds))

  def internalEncoders = {
    val encoders = new ArrayBuffer[(String, ColEncoder[_])]()
    if (maxYear >= minYear) {
      encoders += "Year" -> yearCol
    }
    if (allocateMonth) {
      encoders += "Month" -> monthCol
    }
    if (allocateDay) {
      encoders += "Day" -> dayCol
    }
    if (allocateHr) {
      encoders += "Hour" -> hourCol
    }
    if (allocateMin) {
      encoders += "Minutes" -> minuteCol
    }
    if (allocateSec) {
      encoders += "Seconds" -> secondsCol
    }
    encoders.toVector
  }


  override def set_bits(offset: Int): Int = {
    var off2 = offset
    if(allocateSec)
      off2 = secondsCol.set_bits(off2)
    if(allocateMin)
      off2 = minuteCol.set_bits(off2)
    if(allocateHr)
      off2 = hourCol.set_bits(off2)
    if(allocateDay)
      off2 = dayCol.set_bits(off2)
    if(allocateMonth) {
      off2 = monthCol.set_bits(off2)
      off2 = quarterCol.set_bits(off2)
    }
    if(maxYear >= minYear){
      off2 = yearCol.set_bits(off2)
    }
    bits = offset until off2
    off2
  }

  override def n_bits: Int = {
    var sum = yearCol.n_bits
    if(allocateMonth) {
      sum += quarterCol.n_bits
      sum += monthCol.n_bits
    }
    if(allocateDay)
      sum += dayCol.n_bits
    if(allocateHr)
      sum += hourCol.n_bits
    if(allocateMin)
      sum += minuteCol.n_bits
    if(allocateSec)
      sum += secondsCol.n_bits
    sum
  }

  override def encode_any(v: Any): BigBinary = {
     val dopt = map_f(v)

    var sum = BigBinary(0)
    if(dopt.isDefined) {
      sum = sum + yearCol.encode_any(dopt)
      if (allocateMonth)
        sum = sum + quarterCol.encode_any(dopt) + monthCol.encode_any(dopt)
      if (allocateDay)
        sum = sum + dayCol.encode_any(dopt)
      if (allocateHr)
        sum = sum + hourCol.encode_any(dopt)
      if (allocateMin)
        sum = sum + minuteCol.encode_any(dopt)
      if (allocateSec)
        sum = sum + secondsCol.encode_any(dopt)
    }
    sum
  }

  override def encode_locally(v: Date): Int = ???
  override def decode_locally(i: Int): Date = ???
  override def maxIdx: Int = ???

  override def decode(b: BigBinary): Date = {
    val yD = yearCol.decode(b)
    val m = monthCol.decode(b)
    val d = dayCol.decode(b)
    val hrs = hourCol.decode(b)
    val min = minuteCol.decode(b)
    val sec = secondsCol.decode(b)
    new Date(yD , m, d, hrs, min, sec)
  }

  def queries(): Set[IndexedSeq[Int]] = {
    val ybits = yearCol.bits
    val ymbits = monthCol.bits ++ ybits
    val ymdbits = dayCol.bits ++ ymbits
    val hrbits = hourCol.bits ++ ymdbits
    val hrminBits = minuteCol.bits ++ hrbits

    val yQ = yearCol.queries
    val qQ = quarterCol.queries.flatMap(q => Set(q, q ++ ybits))
    val mQ = monthCol.queries.flatMap(q => Set(q, q ++ ybits))
    val dQ = dayCol.queries.flatMap(q => Set(q, q ++ ymbits))
    val hrQ = hourCol.queries.flatMap(q => Set(q, q ++ ymdbits))
    val minQ = minuteCol.queries.flatMap(q => Set(q, q ++ hrbits))
    val secQ = secondsCol.queries.flatMap(q => Set(q, q ++ hrminBits))
    yQ union qQ union mQ union dQ union hrQ union minQ union secQ
  }
  lazy val myqueries = queries().groupBy(_.size).withDefaultValue(Set())
  override def samplePrefix(size: Int): IndexedSeq[Int] = {
    val size1 = myqueries.keys.filter(_ >= size).min
    val qs = myqueries(size1).toVector
    val idx = Random.nextInt(qs.size)
    qs(idx).takeRight(size)
  }
}

object StaticDateCol {
  def simpleDateFormat(f: String) = {
    val parser = new SimpleDateFormat(f)
    (v: Any) => v match {
      case s: String => parser.synchronized{Try(parser.parse(s))}.toOption
    }
  }

  def fromFile(filename: String, map_f: Any => Option[Date], hasYear: Boolean = false, hasMonth : Boolean = false, hasDay: Boolean = false, hasHr: Boolean = false, hasMin: Boolean = false, hasSec: Boolean = false) = {
    val (ymin, ymax) = if(hasYear) {
      val lines = Source.fromFile(filename).getLines().map(map_f).toSeq
      val yearMax = lines.flatten.map(_.getYear).max
      val yearMin = lines.flatten.map(_.getYear).min
      (yearMin, yearMax)
    } else
      (0, -1)
    new StaticDateCol(map_f, ymin, ymax, hasMonth, hasDay, hasHr, hasMin, hasSec)
  }
}