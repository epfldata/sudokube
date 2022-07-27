package frontend.generators

import frontend.schema._
import util.BigBinary
import util.BigBinaryTools._
/** returns an iterator for sampling a relation. */
case class TupleGenerator(sch: Schema, n: Long, sampling_f: Int => Int,
                          value_f: ValueGenerator = RandomValueGenerator(10)) extends Iterator[(BigBinary, Long)] {
  private var i = 0L

  def hasNext = (i < n)

  def next = {
    if (i % 10000 == 0) print(".")
    if ((n >= 100) && (i % (n / 100) == 0)) print((100 * i) / n + "%")
    i += 1

    val r = sch.columnList.map { case (key, c) => (key, c.sample(sampling_f)) }
    val key =  sch.encode_tuple(r)
    //val key =
    val value = value_f(key)
    (key, value)
  }
}

/**
 * Same as TupleGenerator, but it treats column 0 specially with time-like properties and samples uniformly irrespective
 * of sampling_f
 * */
case class TupleGenerator2(sch: Schema, n: Long, sampling_f: Int => Int,
                           value_f: ValueGenerator = RandomValueGenerator(10)) extends Iterator[(BigBinary, Long)] {
  private var i = 0

  def hasNext = (i < n)

  def next = {
    if (i % 10000 == 0) print(".")
    if ((n >= 100) && (i % (n / 100) == 0)) print((100L * i) / n + "%")

    val r = sch.columnList.zipWithIndex.map { case ((ckey, c), id) =>
      val clen = c.maxIdx + 1
      val cvalue = if (id > 0 || i / clen == n / clen)
        c.sample(sampling_f)
      else
        i % clen
      (ckey, cvalue)
    }
    val key = sch.encode_tuple(r)
    val value = value_f(key)
    assert(value >= 0, s"Value $value must be non-negative")
    i += 1
    (key, value)
  }

}

case class PartitionedTupleGenerator(sch: Schema2, numRows: Long, numParts: Int, sampling_f: Int => Int,
                                     value_f: ValueGenerator = RandomValueGenerator(10)) {
  def data: IndexedSeq[(Int, Iterator[(BigBinary, Long)])] = {
    val chunksize = (numRows/numParts).toInt
    (0 until numParts).map(i => chunksize -> It(i*chunksize, chunksize))
  }

  case class It(start: Int, len: Int) extends Iterator[(BigBinary, Long)] {
    var i = start

    override def hasNext: Boolean = i < (start + len)

    override def next(): (BigBinary, Long) = {
      //val r = sch.columnVector.zipWithIndex.map { case (LD2(ckey, c), id) =>
      //  val cvalue = c.sample(sampling_f)
      //  //println(id + " --> " +cvalue)
      // cvalue
      //}
      //val key = sch.encode_tuple(r)

      val key = sch.columnVector.map { case LD2(_, encoder) =>
        val v0 = sampling_f(encoder.maxIdx + 1)
        BigBinary(v0).pup(encoder.bits)
      }.sum

      //println("Before encoding="+r.mkString(":") + "  after ="+key)
      val value = value_f(key)
      assert(value >= 0, s"Value $value must be non-negative")
      i += 1
      //if(i < start + 10)
      //  println(r.mkString(":") + "  -> " + value)
      (key, value)
    }
  }

}