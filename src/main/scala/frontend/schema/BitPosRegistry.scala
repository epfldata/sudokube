package frontend.schema

@SerialVersionUID(1L)
class BitPosRegistry  extends  Serializable {
  protected var bitpos = 0
  def n_bits = bitpos
  def increment(n: Int)  = {
    val bitposorig = bitpos
    bitpos += n
    bitposorig
  }
}

/** manages an expanding collection of global bit indexes. */
@SerialVersionUID(2L)
class RegisterIdx(val registry: BitPosRegistry) extends Serializable {
  var maxIdx = 0
  var bits = collection.mutable.ArrayBuffer[Int]()
  var bitsMin = 0
  var isRange = true
  def registerIdx(i: Int) {
    if(i > maxIdx) maxIdx = i
    while(i >= (1 << bits.length)) {
      val b = registry.increment(1)
      isRange = isRange && (bits.isEmpty || bits.last == b-1)
      if(bits.isEmpty)
        bitsMin = b
      bits +=  b
    }
  }
}