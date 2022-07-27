package planning

import util.BitUtils


case class NewProjectionMetaData(queryIntersection: Int, cuboidID: Int, cuboidCost: Int, cuboidIntersection: IndexedSeq[Int]) {
  //TODO: Include hamming weight of queryIntersection
  //def accessible_bits = Bits.fromInt(queryIntersection)
  val queryIntersectionSize = BitUtils.sizeOfSet(queryIntersection)
  def sortID(n: Int) = (queryIntersectionSize.toLong << n) + queryIntersection
  def dominates(other: NewProjectionMetaData, cheap: Int = -1) = {
    (!(this eq other)) &&
      ((this.cuboidCost <= other.cuboidCost) ||
        (this.cuboidCost <= cheap)) &&
      ((this.queryIntersection & other.queryIntersection) == other.queryIntersection)
    // this dominates that only if this.queryIntersection bits is superset of that.queryIntersection bits and
    // reading this is cheap or at least cheaper than reading that.
  }
}
