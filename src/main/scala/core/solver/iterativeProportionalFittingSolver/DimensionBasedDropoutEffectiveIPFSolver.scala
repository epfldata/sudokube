package core.solver.iterativeProportionalFittingSolver

import core.solver.iterativeProportionalFittingSolver.IPFUtils.isVariableContained
import util.BitUtils

import scala.collection.mutable
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
   * but will skip a cluster if it contains at least one variable that is not covered by other remaining clusters.
   * The dropout will stop when
   * (1) 95% of the clusters have already been dropped,
   * (2) we have reached the maximum dimensionality of the fetched cuboids (i.e. we will keep all cuboids at the maximum dimensionality),
   * (3) the remaining "coverage", defined as the sum of the number of variables among all clusters, drops below |Q| * (|Q| - 1)
   *     - this threshold can be adjusted — right now the rationale simply comes from the fact that
   *       if we have all possible (|Q|-1)-dimensional cuboids, this "coverage" will be (|Q|-1) * C(|Q|, |Q|-1) = |Q| * (|Q|-1)
   */
  def selectiveDropout(): Unit = {
    val clustersQueue = mutable.PriorityQueue[Cluster]()(Ordering.by(-_.numVariables)) ++ clusters // from low to high dimensionality
    var numDroppedClusters = 0
    val totalNumClusters = clusters.size
    val maxNumDimensions = clusters.map(_.numVariables).max
    var totalCoverage = clusters.toList.map(_.numVariables).sum
    val variableCoverage = (0 until querySize).map(variable => clusters.count(cluster => isVariableContained(variable, cluster.variables))).toBuffer
    print("\t\t\tDropping out cuboids of sizes ")
    breakable { while (clustersQueue.nonEmpty) {
      if (numDroppedClusters >= totalNumClusters * 0.95) {
        break
      }
      val cluster = clustersQueue.dequeue()
      if (cluster.numVariables >= maxNumDimensions) {
        break
      }
      if (BitUtils.IntToSet(cluster.variables).forall(variableCoverage(_) > 1)) {
          // skip if there exists a variable not covered by any other cluster
        if (totalCoverage - cluster.numVariables < querySize * (querySize - 1)) {
          break
        }
        BitUtils.IntToSet(cluster.variables).foreach(variableCoverage(_) -= 1)
        totalCoverage -= cluster.numVariables
        numDroppedClusters += 1
        print(s"${BitUtils.IntToSet(cluster.variables).size}:")
        clusters -= cluster
      }
    } }
    println(s", total of $numDroppedClusters cuboids")
  }
}
