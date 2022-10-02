package planning

import util.BitUtils


case class NewProjectionMetaData(queryIntersection: Int, cuboidID: Int, cuboidCost: Int, cuboidIntersection: IndexedSeq[Int]) {
  //TODO: Include hamming weight of queryIntersection
  //def accessible_bits = Bits.fromInt(queryIntersection)
  val queryIntersectionSize = BitUtils.sizeOfSet(queryIntersection)

  def dominates(other: NewProjectionMetaData, cheap: Int = -1) = {
    (!(this eq other)) &&
      ((this.cuboidCost <= other.cuboidCost) ||
        (this.cuboidCost <= cheap)) &&
      ((this.queryIntersection & other.queryIntersection) == other.queryIntersection)
    // this dominates that only if this.queryIntersection bits is superset of that.queryIntersection bits and
    // reading this is cheap or at least cheaper than reading that.
  }
}

object NewProjectionMetaData {

  /*
      Ordering such that bigger cuboids appear first
   */
  val ordering = new Ordering[NewProjectionMetaData] {
    override def compare(x: NewProjectionMetaData, y: NewProjectionMetaData): Int =  {
      if(x.queryIntersectionSize != y.queryIntersectionSize) y.queryIntersectionSize compare x.queryIntersectionSize
      else if(x.cuboidCost != y.cuboidCost) y.cuboidCost compare x.cuboidCost
      else y.queryIntersection compare x.queryIntersection
    }
  }
}
