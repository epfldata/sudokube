//package ch.epfl.data.sudokube
package frontend.schema.encoders
import frontend.schema.{BitPosRegistry, RegisterIdx}
import util._

import scala.concurrent.{ExecutionContext, Future}

/** Implements a mapping between a local encoder/decoder (which maps between
    numbers 0..(bits.length - 1) and values of type T) and
    a global encoder/decoder that assumes the local values are encoded in bits
    located at indexes "bits".
 */
@SerialVersionUID(1L)
abstract class ColEncoder[T] extends Serializable {
  // abstract members
  def bits: IndexedSeq[Int] //FIXME: TODO: Ensure that bits are in increasing order for all encoders
  def bitsMin : Int  //equivalent to bits.min
  def isRange: Boolean //equivalent to bits.isInstanceOf[Range]
  def encode_locally(v: T) : Int // 0 ...maxIdx for valid values. NULL is encoded as 0
  def decode_locally(i: Int): T //i must be between 0 and maxIdx
  def maxIdx : Int  //maximum possible value returned by encode_locally. Count is given by maxIdx+1
  def queries(): Set[IndexedSeq[Int]]
  def initializeBeforeEncoding(implicit ec: ExecutionContext): Future[Unit] = Future.unit
  def initializeBeforeDecoding(implicit ec: ExecutionContext): Future[Unit] = Future.unit
  def queriesUpto(qsize: Int) : Set[IndexedSeq[Int]] = queries().filter(_.length <= qsize)
  def prefixUpto(size: Int) : Set[IndexedSeq[Int]] = queriesUpto(size) //override for non-prefix columns such as MemCol
  def samplePrefix(size: Int): IndexedSeq[Int] = bits.takeRight(size) //override for nested columns such as Date
  def encode(v: T): BigBinary

  def encode_any(v: Any) : BigBinary = {
    //encode(v.asInstanceOf[T])
    val vt = v.asInstanceOf[T]
    val res = encode(vt)
    res
  }

  def decode(b: BigBinary) : T = {

    val y = b.unpup(bits).toInt
    /*
        val mask = BigBinary(bits.map(x => Big.pow2(x)).sum)
        val f = Bits.mk_project_f(mask, bits.max + 1)
        /*
        private val f = Bits.mk_project_f(
          Bits.mk_list_mask(0 to bits.max, bits.toSet))
        */

        val y = f(i).toInt
    */

    decode_locally(y)
  }

  /**
  returns, for each valuation of a number of q_bits.length bits,
    the decoded values possible for it.
    Example:
    {{{
      // for animals_sch, see the DynamicSchema documentation
      scala> val o = animals_sch.columns("origin")
      o: frontend.ColEncoder[_] = frontend.DynamicSchema\$MemCol@6f3487d0

      scala> o.asInstanceOf[animals_sch.MemCol[Option[String]]].vals
      res0: List[Option[String]] = List(None, Some(North Pole), Some(South America))

      scala> o.bits
      res1: Seq[Int] = List(1, 11)

      scala> o.decode_dim(List(1, 11))
      res2: Option[Seq[Seq[Any]]] = Vector(Vector(None),                // 0
                                           Vector(Some(North Pole)),    // 1
                                           Vector(Some(South America)), // 2
                                           Vector())                    // 3

      scala> o.decode_dim(List(1))
      res3: Option[Seq[Seq[Any]]] =
        Vector(Vector(None, Some(South America)), // least sign. bit is 0
               Vector(Some(North Pole)))          // least sign. bit is 1

      scala> o.decode_dim(List(11))
      res4: Option[Seq[Seq[Any]]] =
        Vector(Vector(None, Some(North Pole)),    // most sign. bit is 0
               Vector(Some(South America)))       // most sign. bit is 1
    }}}
   */
  def decode_dim(q_bits: List[Int]) : Seq[Seq[T]] = {
    val relevant_bits = bits.intersect(q_bits)
    val idxs = relevant_bits.map(x => bits.indexWhere(x == _))

    BitUtils.group_values(idxs, 0 to (bits.length - 1)).map(
      x => x.map(y => {
        try { Some(decode_locally(y.toInt)) }
        catch { case (e: Exception) => None }
      }).flatten
    )
  }

  /** randomly generate a value
  @param sampling function: given range, picks a value in 0 to range - 1
      Examples can be found in object Sampling

      TODO: the range may be smaller. We may not be using all the expressible indexes
      given that many bits.
   */
  def sample(sampling_f: Int => Int): T =
    decode_locally(sampling_f(maxIdx + 1))
}

abstract class StaticColEncoder[T] extends ColEncoder[T] {
  def n_bits: Int
  override def bitsMin: Int = bits.min
  override def isRange: Boolean = true
  var bits: IndexedSeq[Int] = IndexedSeq.empty
  def set_bits(offset: Int) = {
    bits = (offset until offset + n_bits)
    offset + n_bits
  }

  //encoding for static columns, no new value can be added
  override def encode(v: T): BigBinary = {
    if (bits.isEmpty) BigBinary(0)
    else {
      val v0 = encode_locally(v)
      if (v0 > maxIdx) throw new RuntimeException(s"Local encoding $v0 of value $v exceeds expected maximum $maxIdx for encoder of type ${this.getClass.getCanonicalName} and bits ${bits}")
      if (v0 < 0) throw new RuntimeException(s"Local encoding $v0 of value $v is negative for encoder of type ${this.getClass.getCanonicalName} and bits ${bits}")
      if (isRange) {
        val v1 = BigInt(v0)
        val v2 = if (v1 > 0) {
          v1 << bitsMin
        } else BigInt(0)
        BigBinary(v2)
      }
      else {
        val v2 = BigBinary(v0).pup(bits)
        v2
      }
    }
  }
}

abstract class DynamicColEncoder[T](implicit bitPosRegistry: BitPosRegistry) extends ColEncoder[T] {
  val register = new RegisterIdx(bitPosRegistry)
  val isNotNullBit = bitPosRegistry.increment(1)
  val isNotNullBI = BigInt(1) << isNotNullBit
  override def bits: IndexedSeq[Int] = register.bits
  override def bitsMin: Int = register.bitsMin
  override def isRange: Boolean = register.isRange
  override def maxIdx: Int = register.maxIdx
  override def encode(v: T): BigBinary = {
    val data = if (isRange) {
      val v1 = BigInt(encode_locally(v))
      val v2 = if (v1 > 0) {
        v1 << bitsMin
      } else BigInt(0)
      BigBinary(v2)
    }
    else
      BigBinary(encode_locally(v)).pup(bits)
    data + BigBinary(isNotNullBI)
  }

  def decode2(b: BigBinary): Option[T] = {
    if ((b.toBigInt & isNotNullBI) equals isNotNullBI) {
      Some(decode(b))
    } else None
  }
}
