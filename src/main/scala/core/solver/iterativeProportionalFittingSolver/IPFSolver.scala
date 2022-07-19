package core.solver.iterativeProportionalFittingSolver

/**
 * Abstract definition of a iterative proportional fitting solver. To be extended by VaniillaIPFSolver and EffectiveIPFSolver.
 * @author Zhekai Jiang
 * @param querySize Total number of dimensions queried.
 */
abstract class IPFSolver(val querySize: Int) {
  /* private */ val N: Int = 1 << querySize
  /* private */ var clusters: List[Cluster] = List[Cluster]()
  /* private */ var totalDistribution: Array[Double] = Array.fill(N)(1.0 / N) // Initialize to uniform
  var solution: Array[Double] = Array[Double]() // the un-normalized total distribution
  def getSolution: Array[Double] = {
    solution = totalDistribution.map(_ * normalizationFactor)
    solution
  }
  /* private */ val convergenceThreshold: Double = 1e-5
  /* private */ var normalizationFactor: Double = 1.0 // sum of all elements, can be obtained from any marginal distribution

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

  /* private */ def getNumOnesInBinary(n: Int): Int = {
    var numOnes: Int = 0
    var tmp = n
    while (tmp != 0) {
      if (tmp % 2 == 1) {
        numOnes += 1
      }
      tmp /= 2
    }
    numOnes
  }
}
