//package ch.epfl.data.sudokube
package frontend
package schema

import breeze.io.CSVReader
import util._
import util.BigBinaryTools._
import combinatorics.Big
import frontend.schema.encoders.ColEncoder

import java.io._

@SerialVersionUID(6L)
trait Schema extends Serializable {
  // abstract members
  def n_bits: Int

  def columnList: List[(String, ColEncoder[_])]
  protected def encode_column(key: String, v: Any) : BigBinary

  def encode_tuple(t: Seq[(String, Any)]): BigBinary = {
    val cols = Profiler("EncodeColumn"){(t.map { case (key, v) => git(key, v) })}
      Profiler("ColumnSum"){cols.sum}
  }

  def decode_tuple(i: BigBinary): Seq[(String, Any)] =
    columnList.map { case (key, c) => (key, c.decode(i)) }

  def read(filename: String, measure_key: Option[String] = None, map_value : Object => Long = _.asInstanceOf[Long]
          ): Seq[(BigBinary, Long)] = {

    val items = {
      if(filename.endsWith("json"))
        JsonReader.read(filename)
      else if(filename.endsWith("csv")){

        val csv = Profiler("CSVRead"){CSVReader.read(new FileReader(filename))}
        val header = csv.head
        val rows = Profiler("AddingColNames"){csv.tail.map(vs => header.zip(vs).toMap)}
        rows
      } else
        throw new UnsupportedOperationException("Only CSV or JSON supported")
    }
    println("items = " + items + "\n")

    if (measure_key == None) {
      items.map(l => (encode_tuple(l.toList), 1L))
    }
    else {
      items.map(l => {
        val x = l.toMap
        val measure = x.get(measure_key.get).map(map_value).getOrElse(0L)
        (encode_tuple((x - measure_key.get).toList), measure)
      })
    }
  }

  /** saves the schema as a file. Note: Schema.load is used to load, not
      read()!
   */
  def save(filename: String) {
    val file = new File("cubedata/"+filename+"/"+filename+".sch")
    if(!file.exists())
      file.getParentFile.mkdirs()
    val oos = new ObjectOutputStream(new FileOutputStream(file))
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
    val file = new File("cubedata/"+filename+"/"+filename+".sch")
    val ois = new ObjectInputStream(new FileInputStream(file))
    val sch = ois.readObject.asInstanceOf[Schema]
    ois.close
    sch
  }
}

