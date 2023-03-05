package frontend.schema.encoders

import frontend.schema.{BitPosRegistry, RegisterIdx}
import util.{BigBinary, Profiler}

import collection.mutable.ArrayBuffer
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source

class StaticMemCol[T](val n_bits : Int, vals: Seq[T]) extends StaticColEncoder[T] {

  override def queries(): Set[IndexedSeq[Int]] = ???

  private val encode_map: Map[T, Int] =
    vals.zipWithIndex.map{ case (k, i) => (k, i) }.toMap
  private val decode_map: Map[Int, T] =
    vals.zipWithIndex.map{ case (k, i) => (i, k) }.toMap

  def encode_locally(key: T) : Int = encode_map(key)
  def decode_locally(i: Int) : T   = decode_map(i)

  val maxIdx = vals.length - 1
}

//WARNING: map_f must be thread_safe
class LazyMemCol(val filename: String, val map_f: Any => String = _.asInstanceOf[String]) extends StaticColEncoder[String] {
  type T = String
  var encode_map: Map[T, Int] = null
  var decode_map: Vector[T] = null
  val maxIdx =  Source.fromFile(filename).getLines().size-1

  override def initializeBeforeEncoding(implicit ec: ExecutionContext) = {
      Future {
        val lines = Source.fromFile(filename).getLines().map(map_f)
        encode_map = lines.zipWithIndex.toMap
        println(s"Encode $filename loaded")
      }

  }

  override def initializeBeforeDecoding(implicit ec: ExecutionContext) = {
    Future {
      decode_map = Source.fromFile(filename).getLines().map(map_f).toVector
      println(s"Decode $filename loaded")
    }
  }

  override def encode_locally(v: T): Int = encode_map(v)
  override def decode_locally(i: Int): T = if(i < maxIdx) decode_map(i) else null.asInstanceOf[T]
  override def queries(): Set[IndexedSeq[Int]] = Set(Vector(), bits)

  override def prefixUpto(size: Int): Set[IndexedSeq[Int]] = {
    val min = math.min(size, bits.size)
    (0 to min).map{i => bits.takeRight(i)}.toSet
  }

  override def n_bits: Int = if(maxIdx == 0) 0 else math.ceil(math.log(maxIdx) / math.log(2)).toInt
}

/** The domain of the column is elements of type T and NULL (None). */
@SerialVersionUID(5162431377964958396L)
class MemCol[T](init_size: Int = 8
               ) (implicit bitPosRegistry: BitPosRegistry)  extends DynamicColEncoder[T] {


  override def queries(): Set[IndexedSeq[Int]] = Set(Vector(), Vector(isNotNullBit), bits:+ isNotNullBit)

  /* protected */
  var encode_map = new mutable.HashMap[T, Int]
  var decode_map = new collection.mutable.ArrayBuffer[T](init_size)
  register.registerIdx(init_size)

  def this(init_values: Seq[T])(implicit bitPosRegistry: BitPosRegistry) = {
    this(init_values.size)
    init_values.foreach(x => encode_locally(x))
  }

  /** returns index in collection vals. */
  def encode_locally(v: T): Int = {
    if (encode_map.isDefinedAt(v))
      encode_map(v)
    else {
      val newpos = encode_map.size
      encode_map += (v -> newpos)
      decode_map += v
      register.registerIdx(newpos)
      newpos
    }
  }


  //def decode_locally(i: Int, default: T): T = vals.getOrElse(i, default)
  def decode_locally(i: Int): T = decode_map(i)
}

class NestedMemCol[T](partition: T => (T, T), initvalues:Seq[T])(implicit bitPosRegistry: BitPosRegistry) extends ColEncoder[T]  {

  val c1 = new MemCol[T](initvalues.map(x => partition(x)._1).distinct)
  val c2 = new MemCol[T](initvalues.map(x => partition(x)._2).distinct)

  override def queries(): Set[IndexedSeq[Int]] = Set(Vector(), c1.bits, (c1.bits ++ c2.bits))

  override def encode(v: T): BigBinary = {
    val (v1, v2) = partition(v)
     c1.encode(v1) + c2.encode(v2)
  }


  override def bitsMin: Int = ???
  override def isRange: Boolean = ???
  override def maxIdx: Int = ???

  override def bits: IndexedSeq[Int] = c1.bits ++ c2.bits

  override def encode_locally(v: T): Int = ???

  override def decode_locally(i: Int): T = ???
}