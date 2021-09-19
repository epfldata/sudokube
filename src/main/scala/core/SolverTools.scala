//package ch.epfl.data.sudokube
package core


object SolverTools {
  def mk_all_non_neg[T](n_bits: Int)(implicit num: Numeric[T]
  ): collection.mutable.ArrayBuffer[Interval[T]] = {

    val bounds = new collection.mutable.ArrayBuffer[Interval[T]]()
    bounds ++= (1 to n_bits).map(_ => Interval(Some(num.zero), None))
    bounds
  }

  /** Here we encode the constraints that represent the aggregation
      relationships between the available cube projections and the
      query cube we want to construct.
  */
  def mk_constraints[PayloadT](
    projections: Seq[List[Int]],
    n_bits: Int,
    v: Seq[PayloadT]): Seq[(Seq[Int], PayloadT)] =
    projections.map(util.Bits.group_values(_, 0 to (n_bits - 1)).map(
      x => x.map(_.toInt))).flatten.zip(v)
} // end SolverTools


