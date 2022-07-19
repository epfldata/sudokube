package core.solver.iterativeProportionalFittingSolver

import util.Bits

/**
 * Abstract definition of a iterative proportional fitting solver.
 * To be extended by classes of solvers with concrete solving algorithms.
 * @author Zhekai Jiang
 * @param querySize Total number of dimensions queried.
 */
abstract class IPFSolver(val querySize: Int) {
  /* private */ val N: Int = 1 << querySize
  /* private */ var clusters: List[Cluster] = List[Cluster]()
  /* private */ var totalDistribution: Array[Double] = Array.fill(N)(1.0 / N) // Initialize to uniform
  var solution: Array[Double] = Array[Double]() // the un-normalized total distribution
  def getSolution: Array[Double] = {
    val distributionSum = totalDistribution.sum // TODO: Confirm about this normalization, for junction graph only
    solution = totalDistribution.map(_ / distributionSum * normalizationFactor)
    solution
  }
  /* private */ val convergenceThreshold: Double = 1e-5
  /* private */ var normalizationFactor: Double = 1.0 // sum of all elements, can be obtained from any marginal distribution

  //TODO: Change the other add method to directly accept Int instead of overloading
  def add(marginalVariables: Int, marginalDistribution: Array[Double]): Unit = {
    val bitList = Bits.fromInt(marginalVariables).reverse
    add(bitList, marginalDistribution)
  }
  /**
   * Add a new known marginal distribution as a cluster.
   * @param marginalVariables Sequence of marginal variables.
   * @param marginalDistribution Marginal distribution as a one-dimensional array (values encoded as bits of 1 in index).
   */
  def add(marginalVariables: Seq[Int], marginalDistribution: Array[Double]): Unit

  /**
   * Obtain the total distribution
   * @return totalDistribution as a one-dimensional array (values encoded as bits of 1 in index).
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
        val projection = solution.indices.groupBy(i => Bits.project(i, variables)).mapValues {
          idxes => idxes.map(solution(_)).sum
        }.toSeq.sortBy(_._1).map(_._2)
        println(s"Verifying cuboid ${Bits.fromInt(variables).mkString(":")}")
        distribution.map(_ * normalizationFactor).zip(projection).zipWithIndex.foreach { case ((v, p), i) => if (Math.abs(v - p) > 0.0001) println(s"$i :: $v != $p") }
//        assert(distribution.map(_ * normalizationFactor).zip(projection).map { case (v, p) => Math.abs(v - p) <= 0.0001 }.reduce(_ && _))
    }
  }
}