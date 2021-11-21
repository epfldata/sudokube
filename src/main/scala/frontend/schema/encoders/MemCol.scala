package frontend.schema.encoders

import frontend.schema.{BitPosRegistry, RegisterIdx}
import util.{BigBinary, Profiler}

import collection.mutable.ArrayBuffer

class StaticMemCol[T](val n_bits : Int, vals: Seq[T]) extends ColEncoder[T] {

  var bits: Seq[Int] = Nil

  var bitsMin = 0

  def set_bits(offset: Int) {
    bitsMin = offset
    bits = (offset to offset + n_bits - 1)
  }
  override def isRange: Boolean = true
  override def queries(): Set[Seq[Int]] = Set()

  private val vanity_map: Map[T, Int] =
    vals.zipWithIndex.map{ case (k, i) => (k, i) }.toMap
  private val reverse_map: Map[Int, T] =
    vals.zipWithIndex.map{ case (k, i) => (i, k) }.toMap

  override def encode(v: T): BigBinary = if(v.isInstanceOf[Int] && n_bits == 1) {
    val vi = v.asInstanceOf[Int]
    Profiler("LD Encode") {
      val res1 = if (vi == 0)
        BigBinary(0)
      else
        BigBinary(BigInt(1) << bits.head)
      //val res2 =  super.encode(v)
      //assert(res1 == res2)
      res1
    }
  } else super.encode(v)

  def encode_locally(key: T) : Int = vanity_map(key)
  def decode_locally(i: Int) : T   = reverse_map(i)

  def maxIdx = (1 << bits.length) - 1
}

/** The domain of the column is elements of type T and NULL (None). */
class MemCol[T](init_size: Int = 8
               ) (implicit bitPosRegistry: BitPosRegistry)  extends DynamicColEncoder[T] {


  override def queries(): Set[Seq[Int]] = Set(Nil, bits)

  /* protected */
  var encode_map = new collection.mutable.OpenHashMap[T, Int](init_size)
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

class NestedMemCol[T](partition: T => (T, T))(implicit bitPosRegistry: BitPosRegistry) extends ColEncoder[T]  {
  val c1 = new MemCol[T]()
  val c2 = new MemCol[T]()

  override def queries(): Set[Seq[Int]] = Set(Nil, c1.bits.toList, (c1.bits.toList ++ c2.bits.toList))

  override def encode(v: T): BigBinary = {
    val (v1, v2) = partition(v)
     c1.encode(v1) + c2.encode(v2)
  }


  override def bitsMin: Int = ???
  override def isRange: Boolean = ???
  override def maxIdx: Int = ???

  override def bits: Seq[Int] = c1.bits ++ c2.bits

  override def encode_locally(v: T): Int = ???

  override def decode_locally(i: Int): T = ???
}