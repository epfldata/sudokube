package core.solver.iterativeProportionalFittingSolver

import util.BitUtils

/**
 * Abstract definition of a iterative proportional fitting solver.
 * To be extended by classes of solvers with concrete solving algorithms.
 * @author Zhekai Jiang
 * @param querySize Total number of dimensions queried.
 * @param solverName The name of the concrete method (e.g. "Effective IPF", "Loopy IPF"), empty by default,
 *                   to be used in the messages to be printed to the console and as part of the names in the profiler.
 */
abstract class IPFSolver(val querySize: Int, val solverName: String = "") {
  val N: Int = 1 << querySize
  var clusters: Set[Cluster] = Set[Cluster]()
  var oneDimMarginals: Array[Array[Double]] = null
    // To support dropout experiments where some variables are not covered by any cuboid but we can remember the one-dimensional marginal distribution
  var totalDistribution: Array[Double] = Array.fill(N)(1.0 / N) // Initialize to uniform
  var solution: Array[Double] = Array[Double]() // the un-normalized total distribution
 def initializeWithProductDistribution(primaryMoments: Seq[(Int, Double)]) = {
   assert(primaryMoments.head._1 == 0)
   val total = primaryMoments.head._2
   totalDistribution = Array.fill(N)(total)
   val pmArray = new Array[Double](querySize)
   var logh = 0
   primaryMoments.tail.foreach { case (i, m) =>
     assert((1 << logh) == i)
     pmArray(logh) = m / total
     logh += 1
   }
   oneDimMarginals = pmArray.map{theta => Array(1.0-theta, theta)}
   var h = 1
   logh = 0
   while (h < N) {
     val ph = pmArray(logh)
     var i = 0
     while (i < N) {
       var j = 0
       while (j < h) {
         totalDistribution(i + j) *= (1 - ph)
         totalDistribution(i + j + h) *= ph
         j += 1
       }
       i += (h << 1)
     }
     h <<= 1
     logh += 1
   }
   solution = totalDistribution
 }
  def getSolution: Array[Double] = {
    val distributionSum = totalDistribution.sum
      // This normalization only applies for loopy IPF solvers which generate a probability distribution not summing to 1.
      // But this does not really help much because the error would still be very high.
    solution = totalDistribution.map(_ / distributionSum * normalizationFactor)
    solution
  }
  val convergenceThreshold: Double = 1e-5
  var normalizationFactor: Double = 1.0 // sum of all elements, can be obtained from any marginal distribution

  /**
   * Add a new known marginal distribution as a cluster.
   * @param marginalVariables Sequence of marginal variables.
   * @param marginalDistribution Marginal distribution as a one-dimensional array (values encoded as bits of 1 in index).
   */
  def add(marginalVariables: Int, marginalDistribution: Array[Double]): Cluster = {
    normalizationFactor = marginalDistribution.sum
    val cluster = Cluster(marginalVariables, marginalDistribution.map(_ / normalizationFactor))
    clusters += cluster
    cluster
  }

  /**
   * Helper function to add a new known marginal distribution,
   * with variables as a sequence instead of one single integer.
   * @param marginalVariables Sequence of marginal variables.
   * @param marginalDistribution Marginal distribution as a one-dimensional array (values encoded as bits of 1 in index).
   */
  def add(marginalVariables: Seq[Int], marginalDistribution: Array[Double]): Unit = {
    add(BitUtils.SetToInt(marginalVariables), marginalDistribution)
  }

  /**
   * Solve for the total distribution
   * @return Solution (un-normalized) as a one-dimensional array (values encoded as bits of 1 in index).
   */
  def solve(): Array[Double]

  /**
   * Verify whether the marginal distributions given by the solution are consistent with the cuboids.
   * Adapted from the moment solver.
   */
  def verifySolution(): Unit = {
    getSolution
    clusters.foreach {
      case Cluster(variables, distribution) =>
        val projection = solution.indices.groupBy(i => BitUtils.projectIntWithInt(i, variables)).mapValues {
          idxes => idxes.map(solution(_)).sum
        }.toSeq.sortBy(_._1).map(_._2)
        println(s"\t\t\tVerifying cuboid ${BitUtils.IntToSet(variables).mkString(":")}")
        distribution.map(_ * normalizationFactor).zip(projection).zipWithIndex.foreach { case ((v, p), i) => if (Math.abs(v - p) > 0.0001) println(s"\t\t\t\t$i :: $v != $p") }
    }
  }
}