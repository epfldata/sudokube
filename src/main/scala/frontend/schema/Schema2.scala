package frontend.schema

import breeze.io.CSVReader
import frontend.experiments.Tools
import frontend.schema.encoders._
import util._
import util.BigBinaryTools._

import java.io.{File, FileInputStream, FileOutputStream, FileReader, ObjectInputStream, ObjectOutputStream}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source
import scala.util.Random

@SerialVersionUID(3L)
abstract class Dim2(val name: String) extends Serializable {
  def queries: Set[IndexedSeq[Int]]

  def queriesUpto(qsize: Int): Set[IndexedSeq[Int]]

  def numPrefixUpto(size: Int): Array[BigInt]

  def samplePrefix(size: Int): IndexedSeq[Int]

  def maxSize: Int
}

@SerialVersionUID(5066804864072392482L)
case class LD2[T](override val name: String, encoder: ColEncoder[T]) extends Dim2(name) {
  override def queries: Set[IndexedSeq[Int]] = encoder.queries()

  override def queriesUpto(qsize: Int): Set[IndexedSeq[Int]] = encoder.queriesUpto(qsize)

  override def samplePrefix(size: Int): IndexedSeq[Int] = if (size > 0) encoder.samplePrefix(size) else Vector()

  override def numPrefixUpto(size: Int): Array[BigInt] = {
    val sizes = encoder.prefixUpto(size).groupBy(_.size).mapValues(x => BigInt(x.size)).withDefaultValue(BigInt(0))
    val result = (0 to size).map(i => sizes(i)).toArray
    result
  }

  lazy val maxSize = encoder match {
    case d: StaticDateCol => d.myqueries.keys.max //special handling due to all bits never appearing together (eg, due to quarter)
    case _ => encoder.bits.size
  }
}

@SerialVersionUID(-3406868252153216970L)
case class BD2(override val name: String, children: Vector[Dim2], cross: Boolean) extends Dim2(name) {
  def leaves: Vector[LD2[_]] = children.flatMap {
    case b: BD2 => b.leaves
    case x: LD2[_] => Vector(x)
  }

  lazy val maxSize = if (cross)
    children.map(_.maxSize).sum
  else
    children.map(_.maxSize).max


  override def numPrefixUpto(size: Int): Array[BigInt] = if (cross) {
    val init = Array.fill(size + 1)(BigInt(0))
    init(0) += 1
    children.foldLeft(init) { (acc, cur) =>
      val cr = cur.numPrefixUpto(size)
      val result = Array.fill(size + 1)(BigInt(0))
      (0 to size).foreach { s1 =>
        (0 to size - s1).foreach { s2 =>
          result(s1 + s2) += acc(s1) * cr(s2)
        }
      }
      result
    }
  } else {
    val result = Array.fill(size + 1)(BigInt(0))
    result(0) += 1
    children.foreach { c =>
      val cr = c.numPrefixUpto(size)
      (1 to size).foreach(i => result(i) += cr(i))
    }
    result
  }

  override def samplePrefix(size: Int): IndexedSeq[Int] = if (size <= 0) Vector() else {
    assert(size <= maxSize)
    if (cross) {
      val childrenToConsider = collection.mutable.ArrayBuffer[Int]()
      childrenToConsider ++= children.indices
      var bitsLeft = size
      var totalBits = maxSize
      var result = Vector[Int]()
      while (bitsLeft != 0) {
        assert(bitsLeft > 0)
        if (totalBits > bitsLeft) {
          val idx0 = Random.nextInt(childrenToConsider.size) //pick one of the remaining children
          val idx = childrenToConsider(idx0)
          val child = children(idx)
          childrenToConsider -= idx //remove child from being considered again
          val maxBits = Math.min(bitsLeft, child.maxSize)
          var bits = if (maxBits > 0) Random.nextInt(maxBits + 1) else 0
          if (totalBits - child.maxSize < bitsLeft - bits) {
            bits = bitsLeft + child.maxSize - totalBits //so that both sides are equal
          }
          bitsLeft -= bits
          totalBits -= child.maxSize
          result = result ++ child.samplePrefix(bits)
        } else { //must pick all bits from all remaining children
          assert(totalBits == bitsLeft)
          result = result ++ childrenToConsider.map { idx =>
            val child = children(idx)
            child.samplePrefix(child.maxSize)
          }.reduce(_ ++ _)
          bitsLeft = 0
        }
      }
      result
    }
    else {
      var idx = Random.nextInt(children.size)
      while (children(idx).maxSize < size) {
        idx = Random.nextInt(children.size)
      }
      children(idx).samplePrefix(size)
    }
  }


  override def queriesUpto(qsize: Int): Set[IndexedSeq[Int]] = if (qsize <= 0) Set() else {
    if (cross)
      children.foldLeft(Set(IndexedSeq[Int]())) { case (acc, cur) =>
        val cq = cur.queriesUpto(qsize)
        acc.flatMap(q1 => cq.flatMap(q2 => if (q1.size + q2.size <= qsize) Some(q1 ++ q2) else None))
      }
    else
      children.map(_.queriesUpto(qsize)).reduce(_ union _)
  }

  override def queries: Set[IndexedSeq[Int]] = if (cross)
    children.foldLeft(Set(IndexedSeq[Int]())) { case (acc, cur) =>
      val cq = cur.queries
      acc.flatMap(q1 => cq.map(q2 => q1 ++ q2))
    }
  else
    children.map(_.queries).reduce(_ union _)

}

@SerialVersionUID(4L)
class StructuredDynamicSchema(tl: Vector[Dim2])(implicit bitPosRegistry: BitPosRegistry) extends Schema2(tl) {
  override def n_bits = bitPosRegistry.n_bits

  def save(filename: String) {
    val file = new File("cubedata/schema/" + filename + ".sdsch")
    if (!file.exists())
      file.getParentFile.mkdirs()
    val oos = new ObjectOutputStream(new FileOutputStream(file))
    oos.writeObject(this)
    oos.close
  }

  def read(filename: String, measure_key: Option[String] = None, map_value: String => Long = _.toLong) = {
    val data = if (filename.endsWith("csv")) {
      CSVReader.read(new FileReader(filename))
    } else if (filename.endsWith("tsv")) {
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

      val value = if (valueIdx == -1) 1L else map_value(r(valueIdx))

      //println(key.mkString(",") + "      " + encodedkey + "      " + decodedkey.mkString(",") + "   ===> " + value)
      (encodedkey, value)
    })
  }
}

object StructuredDynamicSchema {
  def load(filename: String): StructuredDynamicSchema = {
    val file = new File("cubedata/schema/" + filename + ".sdsch")
    val ois = new ObjectInputStream(new FileInputStream(file))
    val sch = ois.readObject.asInstanceOf[StructuredDynamicSchema]
    ois.close
    sch
  }
}

@SerialVersionUID(4636658400657404607L)
abstract class Schema2(top_level: Vector[Dim2]) extends Serializable {
  val root = new BD2("ROOT", top_level, true)

  def recommended_cube = Tools.params(n_bits, 25)

  lazy val columnVector: Vector[LD2[_]] = root.leaves

  def n_bits: Int

  def encode_column(idx: Int, v: Any): BigBinary = columnVector(idx).encoder.encode_any(v)

  def encode_tuple(tup: IndexedSeq[Any]) = {
    val tsize = tup.size
    val tupmap = Profiler("E1") {
      columnVector.indices.map {
        case i if i < tsize => encode_column(i, tup(i))
        case j => encode_column(j, "")
      }
    }
    Profiler("E2") {
      tupmap.sum
    }
  }

  def decode_tuple(bb: BigBinary) = columnVector.map(c => c.name -> c.encoder.decode(bb))

  def queries = root.queries.toList.sortBy(_.length)

  def queriesUpto(qSize: Int) = root.queriesUpto(qSize).groupBy(_.length)

  def initBeforeEncode() {
    //println("Starting to load dictionary values")
    implicit val ec = ExecutionContext.global
    val futs = columnVector.map {
      _.encoder.initializeBeforeEncoding
    }
    Await.result(Future.sequence(futs), Duration.Inf)
    //println("Dictionary loading complete")
  }

  def initBeforeDecode() {
    //println("Starting to load dictionary values")
    implicit val ec = ExecutionContext.global
    val futs = columnVector.map {
      _.encoder.initializeBeforeDecoding
    }
    Await.result(Future.sequence(futs), Duration.Inf)
    //println("Dictionary loading complete")
  }
}

class StaticSchema2(tl: Vector[Dim2]) extends Schema2(tl) {
  val n_bits = columnVector.foldLeft(0) { (acc, cur) => cur.encoder.asInstanceOf[StaticColEncoder[_]].set_bits(acc) }

}