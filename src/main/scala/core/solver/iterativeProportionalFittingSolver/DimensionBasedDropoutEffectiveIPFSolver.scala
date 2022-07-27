package core.solver.iterativeProportionalFittingSolver

import core.solver.SolverTools.entropy
import core.solver.iterativeProportionalFittingSolver.IPFUtils.{getNumOnesInBinary, isVariablesContained}
import util.BitUtils

import scala.util.control.Breaks.{break, breakable}

/**
 * Variant of the effective IPF where we simply throw out some low-dimensional cuboids.
 * @author Zhekai Jiang
 * @param querySize Total number of dimensions queried.
 * @param solverName The name of the solver, "Dimension-Based Dropout Effective IPF",
 *                   to be used in the messages to be printed to the console and as part of the names in the profiler.
 */
class DimensionBasedDropoutEffectiveIPFSolver(override val querySize: Int,
                                              override val solverName: String = "Dimension-Based Dropout Effective IPF")
  extends DropoutEffectiveIPFSolver(querySize, solverName) {

  /**
   * Select clusters to drop out.
   * At each time, the method will simply throw out one random cluster among those of the lowest dimension
   * but will ignore a cluster if it contains at least one variable that is not covered by other remaining clusters.
   * The dropout will stop when
   * (1) 95% of the clusters have already been dropped,
   * (2) we have reached the maximum dimensionality of the fetched cuboids (i.e. we will keep all cuboids at the maximum dimensionality),
   * (3) the remaining "coverage", defined by the sum of the number of variables among all clusters, drops below |Q| * (|Q| - 1)
   *     - this threshold can be adjusted—right now it simply comes from the fact that
   *       if we have all possible (|Q|-1)-dimensional cuboids, this "coverage" will be (|Q|-1) * C(|Q|, |Q|-1) = |Q| * (|Q|-1)
   * TODO: Some data structures to compute these things more efficiently
   */
  def selectiveDropout(): Unit = {
    var numDroppedClusters: Int = 0
    val totalNumClusters = clusters.length
    val maxNumDimensions = clusters.map(cluster => getNumOnesInBinary(cluster.variables)).max
    var coverage: Int = clusters.map(_.numVariables).sum
    var currentNumDimensions: Int = clusters.map(cluster => getNumOnesInBinary(cluster.variables)).min
    print("\t\t\tDropping out cuboids of sizes ")
    breakable { while (true) {
      if (numDroppedClusters >= totalNumClusters * 0.95) {
        break
      }
      if (currentNumDimensions >= maxNumDimensions) {
        break
      }
      val candidateClustersToDrop = clusters.filter(cluster =>
        cluster.numVariables == currentNumDimensions
          && BitUtils.IntToSet(cluster.variables).forall(variable => (clusters.toSet - cluster).exists(cluster => isVariablesContained(1 << variable, cluster.variables)))
      )
      if (candidateClustersToDrop.isEmpty) {
        currentNumDimensions += 1
      } else {
        val droppedCluster = candidateClustersToDrop.maxBy(cluster => entropy(cluster.distribution))
        if (coverage - droppedCluster.numVariables < querySize * (querySize - 1)) {
          break
        }
        coverage -= droppedCluster.numVariables
        numDroppedClusters += 1
        print(s"${BitUtils.IntToSet(droppedCluster.variables).size}:")
        clusters = clusters.filter(_ != droppedCluster)
      }
    } }
    println(s", total of $numDroppedClusters cuboids")
  }
}
