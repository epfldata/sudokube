//package ch.epfl.data.sudokube
package frontend.schema.encoders
import frontend.schema.RegisterIdx
import util._

/** Implements a mapping between a local encoder/decoder (which maps between
    numbers 0..(bits.length - 1) and values of type T) and
    a global encoder/decoder that assumes the local values are encoded in bits
    located at indexes "bits".
 */
trait ColEncoder[T] extends Serializable {
  // abstract members
  def bits: Seq[Int]
  def encode_locally(v: T) : Int
  def decode_locally(i: Int): T
  def maxIdx : Int
  def queries(): Set[List[Int]]
  def encode(v: T) : BigBinary = BigBinary(encode_locally(v)).pup(bits)
  def encode_any(v: Any) : BigBinary = encode(v.asInstanceOf[T])

  def decode(b: BigBinary) : T = {
    val y = b.toSeq.zipWithIndex.map {
      case(v, i) => {
        val j = bits.indexWhere(_ == i)
        if(j >= 0) Some(v << j) else None
      }
    }.flatten.sum

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

    Bits.group_values(idxs, 0 to (bits.length - 1)).map(
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


