package frontend.schema

import breeze.io.CSVReader
import frontend.experiments.Tools
import frontend.schema.encoders.ColEncoder
import util._
import util.BigBinaryTools._

import java.io.{File, FileInputStream, FileOutputStream, FileReader, ObjectInputStream, ObjectOutputStream}
import scala.io.Source
import scala.util.Random

@SerialVersionUID(3L)
abstract class Dim2(val name: String) extends  Serializable {
  def queries: Set[Seq[Int]]
  def queriesUpto(qsize: Int) : Set[Seq[Int]]
  def sampleQuery(qsize: Int): Option[Seq[Int]]
}
@SerialVersionUID(5066804864072392482L)
case class LD2[T](override val name : String, encoder: ColEncoder[T]) extends Dim2(name) {
  override def queries: Set[Seq[Int]] = encoder.queries()
  override def queriesUpto(qsize: Int): Set[Seq[Int]] = encoder.queriesUpto(qsize)
  override def sampleQuery(qsize: Int): Option[Seq[Int]] = ???
}
@SerialVersionUID(-3406868252153216970L)
case class BD2(override val name: String, children: Vector[Dim2], cross: Boolean) extends Dim2(name) {
  def leaves : Vector[LD2[_]] = children.flatMap {
    case  b: BD2 => b.leaves
    case  x: LD2[_]  => Vector(x)
  }

  override def sampleQuery(qsize: Int) : Option[Seq[Int]] = if(cross){
    val array = Array.fill(children.length)(0)
    (0 until qsize).foreach{ i =>
      val idx = Random.nextInt(children.length)
      array(idx) += 1
    }
    val res = children.indices.foldLeft[Option[Seq[Int]]](Some(Nil)) {
      case (Some(acc), i) => children(i).sampleQuery(array(i)).map(acc ++ _)
      case (None, i) => None
    }
    assert(res.map(_.size == qsize).getOrElse(true))
    res
  } else {
    val idx = Random.nextInt(children.length)
    children(idx).sampleQuery(qsize)
  }


  override def queriesUpto(qsize: Int): Set[Seq[Int]] = if(qsize <= 0) Set() else {
    if(cross)
    children.foldLeft(Set(Seq[Int]())){ case (acc, cur) =>
      acc.flatMap(q1 => cur.queriesUpto(qsize-q1.size).map(q2 => q1 ++ q2))}
    else
      children.map(_.queriesUpto(qsize)).reduce(_ union _)
  }

  override def queries: Set[Seq[Int]] = if(cross)
    children.foldLeft(Set(Seq[Int]())){ case (acc, cur) =>
      val cq = cur.queries
      acc.flatMap(q1 => cq.map(q2 => q1 ++ q2))}
  else
    children.map(_.queries).reduce(_ union _)

}

@SerialVersionUID(4L)
class StructuredDynamicSchema(top_level: Vector[Dim2])(implicit bitPosRegistry: BitPosRegistry) extends Serializable  {
  val root = new BD2("ROOT", top_level, true)

  def recommended_cube = Tools.params(n_bits, 25)
  def n_bits: Int = bitPosRegistry.n_bits
  lazy val columnVector: Vector[LD2[_]] = root.leaves
  def encode_column(idx: Int, v: Any): BigBinary = columnVector(idx).encoder.encode_any(v)

  def encode_tuple(tup: IndexedSeq[Any]) = {
    val tupmap = Profiler("E1") {columnVector.indices.map(i => encode_column(i, tup(i)))}
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