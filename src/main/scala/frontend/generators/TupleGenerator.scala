package frontend.generators

import util.{BigBinary, Profiler}
import frontend.schema._

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
 * Same as TupleGenerator, but it tries to balance values evenly for column 0
 * */
case class TupleGenerator2(sch: Schema, n: Long, sampling_f: Int => Int,
                           value_f: ValueGenerator = RandomValueGenerator(10)) extends Iterator[(BigBinary, Long)] {
  private var i = 0

  def hasNext = (i < n)

  def next = {
    if (i % 10000 == 0) print(".")
    if ((n >= 100) && (i % (n / 100) == 0)) print((100 * i) / n + "%")

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