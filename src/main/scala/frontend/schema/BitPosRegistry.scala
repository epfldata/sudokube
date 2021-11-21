package frontend.schema

class BitPosRegistry {
  protected var bitpos = 0
  def n_bits = bitpos
  def increment(n: Int)  = {
    bitpos += n
    bitpos
  }
}

/** manages an expanding collection of global bit indexes. */
class RegisterIdx(val registry: BitPosRegistry) {
  var maxIdx = 0
  var bits : List[Int] = List()
  var bitsMin = 0
  var isRange = true
  def registerIdx(i: Int) {
    if(i > maxIdx) maxIdx = i
    while(i >= (1 << bits.length)) {
      val b = registry.increment(1)
      isRange = isRange && (bits.isEmpty || bits.head == b-1)
      if(bits.isEmpty)
        bitsMin = b
      bits =  b :: bits
    }
  }
}