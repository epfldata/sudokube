package frontend.schema

import breeze.io.CSVReader
import frontend.experiments.Tools
import frontend.schema.encoders.{ColEncoder, LazyMemCol, StaticColEncoder}
import util._
import util.BigBinaryTools._

import java.io.{File, FileInputStream, FileOutputStream, FileReader, ObjectInputStream, ObjectOutputStream}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
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
      val cq = cur.queriesUpto(qsize)
      acc.flatMap(q1 => cq.flatMap(q2 => if(q1.size + q2.size <= qsize) Some(q1 ++ q2) else None))}
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
class StructuredDynamicSchema(tl: Vector[Dim2])(implicit bitPosRegistry: BitPosRegistry) extends Schema2(tl)  {
  override def n_bits = bitPosRegistry.n_bits

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

abstract class Schema2(top_level: Vector[Dim2]) extends Serializable {
  val root = new BD2("ROOT", top_level, true)
  def recommended_cube = Tools.params(n_bits, 25)
  lazy val columnVector: Vector[LD2[_]] = root.leaves
  def n_bits: Int
  def encode_column(idx: Int, v: Any): BigBinary =  columnVector(idx).encoder.encode_any(v)

  def encode_tuple(tup: IndexedSeq[Any]) = {
    val tsize = tup.size
    val tupmap = Profiler("E1") {columnVector.indices.map{
      case i if i < tsize => encode_column(i, tup(i))
      case j => encode_column(j, "")
    }}
    Profiler("E2"){tupmap.sum}
  }
  def decode_tuple(bb: BigBinary) = columnVector.map(c => c.name -> c.encoder.decode(bb))

  def queries = root.queries.toList.sortBy(_.length)
  def queriesUpto(qSize: Int) = root.queriesUpto(qSize).groupBy(_.length)
  def initBeforeEncode() = {
    println("Starting to load dictionary values")
    implicit val ec = ExecutionContext.global
    val futs = columnVector.map { _.encoder.initializeBeforeEncoding}
    Await.result(Future.sequence(futs), Duration.Inf)
    println("Dictionary loading complete")
  }
  def initBeforeDecode() = {
    println("Starting to load dictionary values")
    implicit val ec = ExecutionContext.global
    val futs = columnVector.map { _.encoder.initializeBeforeDecoding}
    Await.result(Future.sequence(futs), Duration.Inf)
    println("Dictionary loading complete")
  }
}

class StaticSchema2(tl: Vector[Dim2]) extends Schema2(tl) {
  val n_bits = columnVector.foldLeft(0){(acc, cur) => cur.encoder.asInstanceOf[StaticColEncoder[_]].set_bits(acc)}

}