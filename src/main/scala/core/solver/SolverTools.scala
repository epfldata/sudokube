//package ch.epfl.data.sudokube
package core.solver

import core.DataCube
import core.solver.lpp.Interval
import util.{ProgressIndicator, Util}


object SolverTools {
  def primaryMoments(dc: DataCube, showProgress: Boolean = true): (Long, Array[Long]) = {
    val nbits = dc.index.n_bits
    var total = 0L
    val moments1D = Array.fill(nbits)(0L)
    val pi = new ProgressIndicator(nbits, "Primary Moment Computation", showProgress)
    moments1D.indices.foreach { i =>
      val l = dc.index.prepareNaive(Vector(i))
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
  def preparePrimaryMomentsForQuery[T](q: Seq[Int], primaryMoments: (Long, Array[Long]))(implicit num: Fractional[T]): Seq[(Int, T)] = {

    val m1D = q.zipWithIndex.map { case (b, i) => (1 << i) -> Util.fromLong(primaryMoments._2(b)) }
    (0 -> Util.fromLong(primaryMoments._1)) +: m1D
  }
  def intervalPrecision[T](trueResult: Array[Double], bounds: IndexedSeq[Interval[T]])(implicit num: Fractional[T]) = {
    val cumulativeSpan = num.toDouble(bounds.map { i => num.minus(i.ub.get, i.lb.get) }.sum)
    val total = trueResult.sum
    cumulativeSpan / total
  }
  def error[T](naive: Array[Double], solver: Array[T])(implicit num: Fractional[T]) = {
    //assumes naive values can fit in Long without any fraction or overflow
    val length = naive.length
    assert(solver.length == length)
    import Util.fromLong
    val deviation = (0 until length).map(i => num.abs(num.minus(fromLong(naive(i).toLong), solver(i)))).sum
    val sum = naive.sum
    if (sum == 0) num.toDouble(deviation) else num.toDouble(deviation) / sum
  }

  def errorMax[T](naive: Array[Double], solver: Array[T])(implicit num: Fractional[T]) = {
    //assumes naive values can fit in Long without any fraction or overflow
    val length = naive.length
    assert(solver.length == length)
    import Util.fromLong
    val deviation = (0 until length).map(i => num.abs(num.minus(fromLong(naive(i).toLong), solver(i)))).max
    //val sum = naive.sum
    num.toDouble(deviation)
  }

  def errorPlus[T](naive: Array[Double], solver: Array[T])(implicit num: Fractional[T]) = {
    //assumes naive values can fit in Long without any fraction or overflow
    val length = naive.length
    assert(solver.length == length)
    import Util.fromLong
    val deviation = (0 until length).map(i => num.abs(num.minus(fromLong(naive(i).toLong), solver(i)))).sum
    val maxNormalizedDeviation = (0 until length).map { i =>
      val numNaive = fromLong(naive(i).toLong)
      val dev = num.abs(num.minus(numNaive, solver(i)))
      val ratio = num.div(dev, num.max(numNaive, num.one))
      (naive(i), num.toDouble(solver(i)), num.toDouble(dev), ratio)
    }.sortBy(_._4).last
    val nd = maxNormalizedDeviation
    val totalDev = num.toDouble(deviation)
    val sum = naive.sum
    val error = totalDev / (1.0 max sum)
    s"$sum, ${num.toDouble(solver.sum)}, $totalDev, $error, ${nd._1}, ${nd._2}, ${nd._3}, ${nd._4}"
  }


  def errorTT[T](naive: Array[T], solver: Array[T])(implicit num: Fractional[T]) = {
    //assumes naive values can fit in Long without any fraction or overflow
    val length = naive.length
    assert(solver.length == length)
    import Util.fromLong
    val deviation = (0 until length).map(i => num.abs(num.minus(naive(i), solver(i)))).sum
    val sum = naive.sum
    if (sum equals num.zero) 0.0 else num.toDouble(num.div(deviation, sum))
  }

  def entropyBase2(result: Array[Double]) = {
    import math.log
    val sum = result.sum
    val ps = result.map(x => x / sum).map(p => if (p == 0.0) 0.0 else p * log(p) / log(2))
    -ps.sum
  }
  def normalizedEntropyBase2(result: Array[Double]) = {
    import math.log
    val sum = result.sum
    val ps = result.map(x => x / sum).map(p => if (p == 0.0) 0.0 else p * log(p) / log(2))
    val maxEntropy = log(result.size) / log(2)
    -ps.sum / maxEntropy
  }
  def entropy(result: Array[Double]) = {
    val sum = result.sum
    val ps = result.map(x => x / sum).map(p => if (p == 0.0) 0.0 else p * math.log(p))
    -ps.sum
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
    projections: Seq[Int],
    v: Seq[PayloadT]): Seq[(Seq[Int], PayloadT)] =
    projections.map(util.BitUtils.group_values_Int(_, n_bits).map(
      x => x.map(_.toInt))).flatten.zip(v)
} // end SolverTools


