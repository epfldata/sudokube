package frontend.schema

import breeze.io.CSVReader
import frontend.schema.encoders.ColEncoder
import util._
import util.BigBinaryTools._

import java.io.{File, FileInputStream, FileOutputStream, FileReader, ObjectInputStream, ObjectOutputStream}
import scala.io.Source

abstract class Dim2(val name: String) extends  Serializable {
  def queries: Set[Seq[Int]]
}
case class LD2[T](override val name : String, val encoder: ColEncoder[T]) extends Dim2(name) {
  override def queries: Set[Seq[Int]] = encoder.queries()
}
case class BD2(override val name: String, children: Vector[Dim2], cross: Boolean) extends Dim2(name) {
  def leaves : Vector[LD2[_]] = children.flatMap {
    case  b: BD2 => b.leaves
    case  x: LD2[_]  => Vector(x)
  }

  override def queries: Set[Seq[Int]] = if(cross)
    children.foldLeft(Set(Seq[Int]())){ case (acc, cur) => acc.flatMap(q1 => cur.queries.map(q2 => q1 ++ q2))}
  else
    children.map(_.queries).reduce(_ union _)

}

class StructuredDynamicSchema(top_level: Vector[Dim2])(implicit bitPosRegistry: BitPosRegistry) extends Serializable  {
  val root = new BD2("ROOT", top_level, true)

  def n_bits: Int = bitPosRegistry.n_bits
  lazy val columnVector: Vector[LD2[_]] = root.leaves
  def encode_column(idx: Int, v: Any): BigBinary = columnVector(idx).encoder.encode_any(v)

  def encode_tuple(tup: IndexedSeq[Any]) = {
    val tupmap = Profiler("E1") {tup.indices.map(i => encode_column(i, tup(i)))}
      Profiler("E2"){tupmap.sum}
  }
  def decode_tuple(bb: BigBinary) = columnVector.map(c => c.name -> c.encoder.decode(bb))

  def queries = root.queries.toList.sortBy(_.length)
  /** saves the schema as a file. Note: Schema.load is used to load, not
      read()!
   */
  def save(filename: String) {
    val file = new File("cubedata/schema/"+filename+".sdsch")
    if(!file.exists())
      file.getParentFile.mkdirs()
    val oos = new ObjectOutputStream(new FileOutputStream(file))
    oos.writeObject(this)
    oos.close
  }

  def read(filename: String, measure_key: Option[String] = None, map_value : String => Long = _.toLong) = {
    val data = if(filename.endsWith("csv")) {
      CSVReader.read(new FileReader(filename))
    } else if(filename.endsWith("tsv")) {
      Source.fromFile(filename, "utf-8").getLines().map(_.split("\t").toVector).toVector
    } else {
     throw new UnsupportedOperationException
    }
    val header = data.head
    val keyIdx = columnVector.map(c => header.indexOf(c.name))
    val valueIdx = measure_key.map(k => header.indexOf(k)).getOrElse(-1)
    data.tail.map(r => {
      val key = keyIdx.map(i => r(i))
      val encodedkey = encode_tuple(key)
      //val decodedkey = decode_tuple(encodedkey)

      val value = if(valueIdx == -1) 1L else map_value(r(valueIdx))

      //println(key.mkString(",") + "      " + encodedkey + "      " + decodedkey.mkString(",") + "   ===> " + value)
      (encodedkey, value)
    })
  }
}

object StructuredDynamicSchema {
  def load(filename: String): StructuredDynamicSchema = {
    val file = new File("cubedata/schema/"+filename+".sdsch")
    val ois = new ObjectInputStream(new FileInputStream(file))
    val sch = ois.readObject.asInstanceOf[StructuredDynamicSchema]
    ois.close
    sch
  }
}