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
  protected def toSeqN(n: Int) : Seq[Int] = {
     var bi = toBigInt % (1 << n)
     var n2 = n
     var l1 = List[Int]()
     while(n2 > 0) {
       l1 = (bi % 2).toInt :: l1
       bi = bi >> 1
       n2 -= 1
     }
     l1.reverse
   }

  def toSeq : Seq[Int] = {
    def toSeq0(i: BigInt) : List[Int] = {
      if(i <= 0) List[Int]()
      else (i % 2).toInt :: toSeq0(i >> 1)
    }

    assert(toBigInt >= 0)
    toSeq0(toBigInt)
  }

  def toCharArray(n_bits: Int) : Array[Char] = {
    val space = math.ceil(n_bits.toDouble/8).toInt
    val ca = Util.mkAB[Char](space, _ => 0)

    var y = toBigInt
    var i = 0
    while(y > 0) {
      ca(i) = (y % 256).toInt.toChar
      y = y / 256
      i += 1
    }
    ca.toArray
  }

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
  def pup(bit_indexes: Seq[Int]) : BigBinary =
    if(bit_indexes.isInstanceOf[Range]) {
     BigBinary(toBigInt <<  bit_indexes.head)
    }
    else {
      BigBinary(toSeqN(bit_indexes.length).zip(bit_indexes).map {
        case (b, i) => BigInt(b) << i
      }.sum)
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


