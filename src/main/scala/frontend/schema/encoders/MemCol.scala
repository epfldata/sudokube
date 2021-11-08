package frontend.schema.encoders

import frontend.schema.{BitPosRegistry, RegisterIdx}
import util.BigBinary

import collection.mutable.ArrayBuffer

/** The domain of the column is elements of type T and NULL (None). */
class MemCol[T](init_vals: List[T] = List[T]()
               ) extends ColEncoder[T] with RegisterIdx {


  override def queries(): Set[List[Int]] = Set(Nil, bits)

  /* protected */
  var encode_map = collection.mutable.Map[T, Int]()
  var decode_map = collection.mutable.ArrayBuffer[T]()
  init_vals.foreach {
    encode_locally(_)
  }

  /** returns index in collection vals. */
  def encode_locally(v: T): Int = {
    if (encode_map.isDefinedAt(v))
      encode_map(v)
    else {
      val newpos = encode_map.size
      encode_map += (v -> newpos)
      decode_map += v
      registerIdx(newpos)
      newpos
    }
  }




  //def decode_locally(i: Int, default: T): T = vals.getOrElse(i, default)
  def decode_locally(i: Int): T = decode_map(i)
}

class NestedMemCol[T](partition: T => (T, T)) extends ColEncoder[T] with RegisterIdx {
  val c1 = new MemCol[T]()
  val c2 = new MemCol[T]()

  override def queries(): Set[List[Int]] = Set(Nil, c1.bits, (c1.bits ++ c2.bits))

  override def encode(v: T): BigBinary = {
    val (v1, v2) = partition(v)
     c1.encode(v1) + c2.encode(v2)
  }

  override def setRegistry(r: BitPosRegistry): Unit = {
    c1.setRegistry(r)
    c2.setRegistry(r)
  }

  override def refreshBits: Unit = {
    bits = c1.bits ++ c2.bits
  }

  override def encode_locally(v: T): Int = ???

  override def decode_locally(i: Int): T = ???
}