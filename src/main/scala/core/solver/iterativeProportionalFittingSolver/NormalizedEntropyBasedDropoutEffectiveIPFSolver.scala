package core.solver.iterativeProportionalFittingSolver

import core.solver.SolverTools.entropy
import core.solver.iterativeProportionalFittingSolver.IPFUtils.isVariableContained
import util.BitUtils

import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

/**
 * Variant of the effective IPF where we throw out some cuboids with high ("normalized") entropies.
 * @author Zhekai Jiang
 * @param querySize Total number of dimensions queried.
 * @param solverName The name of the solver, "Normalized-Entropy-Based Dropout Effective IPF",
 *                   to be used in the messages to be printed to the console and as part of the names in the profiler.
 */
class NormalizedEntropyBasedDropoutEffectiveIPFSolver(override val querySize: Int,
                                                      override val solverName: String = "Normalized-Entropy-Based Dropout Effective IPF")
  extends DropoutEffectiveIPFSolver(querySize, solverName) {

  /**
   * Select clusters to drop out.
   * The clusters will be ranked according to its entropy normalized by the maximum possible entropy at its dimensionality (- N * N log 1/N = -log 1/N),
   * in a decreasing order (since we would prefer low-entropy ones).
   * At each time, the method will simply throw out the cluster with the lowest "normalized" entropy
   * but will skip a cluster if it contains at least one variable that is not covered by other remaining clusters.
   * The dropout will stop when either
   * (1) 95% of the clusters have already been dropped, or
   * (2) the remaining "coverage", defined as the sum of the number of variables among all clusters, drops below |Q| * (|Q| - 1)
   *     - this threshold can be adjusted — right now the rationale simply comes from the fact that
   *       if we have all possible (|Q|-1)-dimensional cuboids, this "coverage" will be (|Q|-1) * C(|Q|, |Q|-1) = |Q| * (|Q|-1)
   */
  def selectiveDropout(): Unit = {
    val clustersQueue = mutable.PriorityQueue[Cluster]()(Ordering.by(cluster => (
      normalizedEntropy(cluster), // high to low entropy
      -cluster.numVariables // low dimensionality to high dimensionality in case of tie
    ))) ++ clusters
    var numDroppedClusters = 0
    val totalNumClusters = clusters.size
    var totalCoverage = clusters.toList.map(_.numVariables).sum
    val variableCoverage = (0 until querySize).map(variable => clusters.count(cluster => isVariableContained(variable, cluster.variables))).toBuffer
    print("\t\t\tDropping out cuboids of sizes (entropy ratios) ")
    breakable { while (clustersQueue.nonEmpty) {
      if (numDroppedClusters >= totalNumClusters * 0.95) {
        break
      }
      val cluster = clustersQueue.dequeue()
      if (BitUtils.IntToSet(cluster.variables).forall(variableCoverage(_) > 1)) {
        // skip if there exists a variable not covered by any other cluster
        if (totalCoverage - cluster.numVariables < querySize * (querySize - 1)) {
          break
        }
        BitUtils.IntToSet(cluster.variables).foreach(variableCoverage(_) -= 1)
        totalCoverage -= cluster.numVariables
        numDroppedClusters += 1
        print(f"${BitUtils.IntToSet(cluster.variables).size} (${normalizedEntropy(cluster)}%.2f), ")
        clusters -= cluster
      }
    } }
    println(s"total of $numDroppedClusters cuboids")
  }

  /**
   * Helper function to compute the ratio of the entropy of a cluster relative to the maximum possible entropy at its dimensionality.
   * @param cluster The cluster.
   * @return The ratio of the entropy of a cluster relative to the maximum possible entropy at its dimensionality.
   */
  def normalizedEntropy(cluster: Cluster): Double = entropy(cluster.distribution) / (-math.log(1.0 / (1 << cluster.numVariables)))
}
