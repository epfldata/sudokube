package planning

case class NewProjectionMetaData(queryIntersection: Int, cuboidID: Int, cuboidCost: Int, cuboidIntersection: Seq[Int]) {
  def dominates(other: NewProjectionMetaData, cheap: Int = -1) = {
    (!(this eq other)) &&
      ((this.cuboidCost <= other.cuboidCost) ||
        (this.cuboidCost <= cheap)) &&
      ((this.queryIntersection & other.queryIntersection) == other.queryIntersection)
    // this dominates that only if this.queryIntersection bits is superset of that.queryIntersection bits and
    // reading this is cheap or at least cheaper than reading that.
  }
}
