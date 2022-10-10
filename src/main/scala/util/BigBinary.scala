//package ch.epfl.data.sudokube
package util


/** the least significant bit has index 0
*/
case class BigBinary(val toBigInt: BigInt) {

  assert(toBigInt >= 0)

  def binary_digits = {
    var l = toBigInt.toString.length
    var d = 0
    var x = toBigInt
    while(x > 1) {
      while(x >> l > 0) {
        x = x >> l
        d += l
      }
      l = l / 2
    }
    d + 1
  }

  /** get bit at position pos */
  def apply(pos: Int) : Int = ((toBigInt >> pos) % 2).toInt

  def valueOf(pos: List[Int]) = pos.reverse.foldLeft(0)((acc, cur) => 2 * acc + apply(cur))

  /** get sequence of the n least significant bits
      throws exception if n > toBigInt.length * wordlen
  */
  protected def toSeqN(n: Int) : Seq[Int] = (0 to n - 1).map(apply(_))

  def toSeq : Seq[Int] = {
    def toSeq0(i: BigInt) : List[Int] = {
      if(i <= 0) List[Int]()
      else (i % 2).toInt :: toSeq0(i >> 1)
    }

    assert(toBigInt >= 0)
    if(toBigInt == 0) List(0) else toSeq0(toBigInt)
  }

  /** Example:
      {{{
      scala> val x = (10 << 16) + (3<<8) + 255
      x: Int = 656383
      scala> BigBinary(x).toByteArray(24)
      res0: Array[Byte] = Array(-1, 3, 10)
      }}}
  */
  def toByteArray(n_bits: Int) : Array[Byte] = {
    val space = BitUtils.bitToBytes(n_bits) //ceil(n_bits.toDouble/8).toInt
    val ca = Array.fill[Byte](space)(0)

    var y = toBigInt
    var i = 0
    while(y > 0) {
      ca(i) = (y & 0xff).toByte
      y = y >> 8
      i += 1
    }
    ca
  }

  // converts the number to a binary string of size n with leading zeroes.
  def toPaddedString(n: Int) = {
    val d = toSeq.length
    var s = ""
    var i = 0
    while (i < (n-d)) {
      if ((n - i) % 8 == 0) s = s + ' '
      s = s + '0'
      i += 1
    }
    for (c <- toSeq.reverse) {
      if ((n - i) % 8 == 0) s = s + ' '
      s = s + (if (c == 0) '0' else '1')
      i += 1
    }
    s
  }

  // converts the number to a binary string, without leading zeroes.
  override def toString = {
    val d = binary_digits
    var s = ""
    var i = 0
    for(c <- toSeq.reverse) {
      if((d - i) % 8 == 0) s = s + ' '
      s = s + (if(c == 0) '0' else '1')
      i += 1
    }
    s
  }

  def toInt = this.toBigInt.toInt
  def |(that: BigBinary) = BigBinary(this.toBigInt | that.toBigInt)
  def +(that: BigBinary) = BigBinary(this.toBigInt + that.toBigInt)
  def &(that: BigBinary) = BigBinary(this.toBigInt & that.toBigInt)

  /** returns the number of ones in the binary representation
  */
  def hamming_weight = toSeq.filter(_ == 1).length

  /** permute and un-project (maybe call it decode?)
      {{{
      abc.pup(List(0, 2, 5)) = a00b0c  ... a,b,c bits
      abc.pup(List(2, 0, 5)) = a00c0b
      ab.pup(List(0, 0))    = a+b
      }}}
  */
  def pup(bit_indexes: Seq[Int]): BigBinary = {
    if (bit_indexes.isInstanceOf[Range]) {
      val mask = (1 << bit_indexes.size) - 1
      //Assumes there are no ones in toBigInt beyond bit_indexes.length
      BigBinary((toBigInt & mask) << bit_indexes.head)
    }
    else {
      val bi = bit_indexes.foldLeft((toBigInt, BigInt(0))) {
        case ((num, res), idx) =>
          val dig = num % 2
          val newres = res + (dig << idx)
          val newnum = num >> 1
          (newnum, newres)
      }
      BigBinary(bi._2)
    }
  }

  /** opposite of pup. Extracts values at specific bits from bigbinary
   * {{{
   *a00b0c.pup(List(0, 2, 5)) = abc  ... a,b,c bits
   *a00c0b.pup(List(2, 0, 5)) = abc
   * }}}
   */
  def unpup(bit_indexes: Seq[Int]): BigBinary = {
    val bi = if(bit_indexes.isEmpty) BigInt(0)
    else if (bit_indexes.isInstanceOf[Range]) {
      toBigInt >> bit_indexes.head & ((1 << bit_indexes.size) - 1)
    }
    else {
      toSeq.zipWithIndex.map {
        case (v, i) => {
          val j = bit_indexes.indexWhere(_ == i)
          if (j >= 0) Some(BigInt(v) << j) else None
        }
      }.flatten.sum
    }
    BigBinary(bi)
  }
}


object BigBinaryTools {
  implicit object BigBinaryOps extends Numeric[BigBinary] {
    override def zero = BigBinary(0)

    def    plus(x: BigBinary, y: BigBinary) = x + y
    def   minus(x: BigBinary, y: BigBinary) = BigBinary(x.toBigInt - y.toBigInt)
    def   times(x: BigBinary, y: BigBinary) = BigBinary(x.toBigInt * y.toBigInt)
    def compare(x: BigBinary, y: BigBinary): Int = x.toBigInt compare y.toBigInt

    def fromInt(x: Int): BigBinary = BigBinary(x)
    def   negate(x: BigBinary): BigBinary = BigBinary(-x.toBigInt)
    def toDouble(x: BigBinary): Double    = x.toBigInt.toDouble
    def  toFloat(x: BigBinary): Float     = x.toBigInt.toFloat
    def    toInt(x: BigBinary): Int       = x.toBigInt.toInt
    def   toLong(x: BigBinary): Long      = x.toBigInt.toLong
  }
}


