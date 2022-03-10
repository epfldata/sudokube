//package ch.epfl.data.sudokube
package core

import core.solver.MomentTransformer
import util.ProgressIndicator


object SolverTools {
  def primaryMoments(dc: DataCube, showProgress: Boolean = true): (Long, Array[Long]) = {
    val nbits = dc.m.n_bits
    var total = 0L
    val moments1D = Array.fill(nbits)(0L)
    val pi = new ProgressIndicator(nbits, "Primary Moment Computation", showProgress)
    moments1D.indices.foreach { i =>
      val l = dc.m.prepare(List(i), nbits, nbits)
      val fetched = dc.fetch(l).map(_.smLong)
      if (i == 0)
        total = fetched.sum
      val moment = fetched(1)
      moments1D(i) = moment
      pi.step
    }
    (total, moments1D)
  }

  //q is assumed to be sorted
  def preparePrimaryMomentsForQuery(q: Seq[Int], primaryMoments:(Long, Array[Long])) : Seq[(Int, Double)] = {
    val m1D = q.zipWithIndex.map{case (b,i) => (1 << i) -> primaryMoments._2(b).toDouble}
    m1D :+ (0 -> primaryMoments._1.toDouble)
  }

  def fastMoments(naive: Array[Double]): Array[Double] = {
    val result = naive.clone()
    val N = naive.size
    var h = 1
    while (h < N) {
      (0 until N by h * 2).foreach { i =>
        (i until i + h).foreach { j =>
          val sum = result(j) + result(j + h)
          result(j) = sum
        }
      }
      h *= 2
    }
    result
  }


  def error(naive: Array[Double], solver: Array[Double]) = {
    val length = naive.length
    val deviation = (0 until length).map(i => Math.abs(naive(i) - solver(i))).sum
    val sum = naive.sum
    deviation / sum
  }

  /// creates initial intervals [0, +\infty) for each variable.
  def mk_all_non_neg[T](n_vars: Int)(implicit num: Numeric[T]
  ): collection.mutable.ArrayBuffer[Interval[T]] = {

    val bounds = new collection.mutable.ArrayBuffer[Interval[T]]()
    bounds ++= (1 to n_vars).map(_ => Interval(Some(num.zero), None))
    bounds
  }

  /** Here we encode the constraints that represent the aggregation
   * relationships between the available cube projections and the
   * query cube we want to construct.
   *
   * Example:
   * {{{
   *scala> core.SolverTools.mk_constraints(List(List(0,1), List(2)), 3, List(1,2,3,4,5,6))
   *res0: Seq[(Seq[Int], Int)] = List(
   *(Vector(0, 4),1), (Vector(1, 5),2), (Vector(2, 6),3), (Vector(3, 7),4),
   *(Vector(0, 1, 2, 3),5), (Vector(4, 5, 6, 7),6))
   * }}}
   * This corresponds to the equation system
   * x0 + x4 = 1
   * x1 + x5 = 2
   * ...
   * x4 + x5 + x6 + x7 = 6.
   *
   * Note that the sums 1-6 here do not make much sense.
   */
  def mk_constraints[PayloadT](
                                n_bits: Int,
                                projections: Seq[Seq[Int]],
                                v: Seq[PayloadT]): Seq[(Seq[Int], PayloadT)] =
    projections.map(util.Bits.group_values(_, 0 to (n_bits - 1)).map(
      x => x.map(_.toInt))).flatten.zip(v)
} // end SolverTools


