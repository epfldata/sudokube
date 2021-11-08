package frontend.schema

trait BitPosRegistry {
  protected var bitpos = 0
  def increment(n: Int)  = {
    bitpos += n
    bitpos
  }
}

/** manages an expanding collection of global bit indexes. */
trait RegisterIdx {
  var maxIdx = 0
  var registry: BitPosRegistry = null
  def setRegistry(r: BitPosRegistry) = registry = r
  var bits : List[Int] = List()

  def refreshBits = {}
  def registerIdx(i: Int) {
    if(i > maxIdx) maxIdx = i
    while(i >= (1 << bits.length)) {
      bits =  registry.increment(1) :: bits
    }
  }
}