//package ch.epfl.data.sudokube
package frontend
package schema

import util._
import util.BigBinaryTools._
import combinatorics.Big
import java.io._


trait Schema extends Serializable {
  def n_bits: Int

  def columnList: List[(String, ColEncoder[_])]
  protected def encode_column(key: String, v: Any) : BigBinary

  def encode_tuple(t: Seq[(String, Any)]): BigBinary =
    (t.map { case (key, v) => encode_column(key, v) }).sum

  def decode_tuple(i: BigBinary): Seq[(String, Any)] =
    columnList.map { case (key, c) => (key, c.decode(i)) }

  case class TrendGenerator(n: Int, sampling_f: Int => Int, trendCols: List[Int], filterCols: List[Int], filterVal : Int) extends Iterator[(BigBinary, Int)] {
    private var i = 0
    val out = new PrintWriter("basecube.txt")
    def hasNext = {
      val res = (i < n)
      if(!res) out.close()
      res
    }

    def next = {
      if (i % 10000 == 0) print(".")
      if ((n >= 100) && (i % (n / 100) == 0)) print((100 * i) / n + "%")
      i += 1

      val r = columnList.map { case (key, c) => (key, c.sample(sampling_f)) }
      val key = encode_tuple(r)
      val filter = key.valueOf(filterCols)
      var value = scala.util.Random.nextInt(10)
      if (filter == filterVal)
        value += (key.valueOf(trendCols) * 100) / (1 << trendCols.length)
      //if(value > 0 ) out.println(key.toPaddedString(n_bits) + " -> " + value)
      (key, value)
    }
  }


  /** returns an iterator for sampling a relation. */
  case class TupleGenerator(n: Int, sampling_f: Int => Int
                           ) extends Iterator[(BigBinary, Int)] {
    private var i = 0

    def hasNext = (i < n)

    def next = {
      if (i % 10000 == 0) print(".")
      if ((n >= 100) && (i % (n / 100) == 0)) print((100 * i) / n + "%")
      i += 1

      val r = columnList.map { case (key, c) => (key, c.sample(sampling_f)) }

      (encode_tuple(r), scala.util.Random.nextInt(10))
    }
  }

  def read(filename: String, measure_key: Option[String] = None
          ): List[(BigBinary, Int)] = {

    val items = JsonReader.read(filename)

    if (measure_key == None) {
      items.map(l => (encode_tuple(l.toList), 1))
    }
    else {
      items.map(l => {
        val x = l.toMap
        val measure: Int = x.getOrElse(measure_key.get, 0).asInstanceOf[Int]
        (encode_tuple((x - measure_key.get).toList), measure)
      })
    }
  }

  /** saves the schema as a file. Note: Schema.load is used to load, not
      read()!
   */
  def save(filename: String) {
    val oos = new ObjectOutputStream(new FileOutputStream(filename))
    oos.writeObject(this)
    oos.close
  }

  def decode_dim(q_bits: List[Int]): Seq[Seq[String]] = {
    val relevant_cols = columnList.filter(!_._2.bits.intersect(q_bits).isEmpty)
    val universe = relevant_cols.map {
      case (_, c) => c.bits
    }.flatten.sorted

    Bits.group_values(q_bits, universe).map(
      x => relevant_cols.map {
        case (key, c) => {
          val l = x.map(y =>
            try {
              val w = c.decode(y) match {
                case Some(u) => u.toString
                case None => "NULL"
                case u => u.toString
              }
              Some(w)
            }
            catch {
              case e: Exception => None
            }
          ).flatten.toSet.toList.sorted // duplicate elimination

          if (l.length == 1) key + "=" + l(0)
          else key + " in " + l
        }
      })
  }
}


object Schema {
  def load(filename: String): Schema = {
    val ois = new ObjectInputStream(new FileInputStream(filename))
    val sch = ois.readObject.asInstanceOf[Schema]
    ois.close
    sch
  }
}

