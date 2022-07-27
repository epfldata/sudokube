package core.solver.iterativeProportionalFittingSolver

import core.solver.SolverTools.entropy
import core.solver.iterativeProportionalFittingSolver.IPFUtils.isVariablesContained
import util.BitUtils

import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

/**
 * Variant of the effective IPF where we throw out some cuboids with high entropies.
 * @author Zhekai Jiang
 * @param querySize Total number of dimensions queried.
 * @param solverName The name of the solver, "Entropy-Based Dropout Effective IPF",
 *                   to be used in the messages to be printed to the console and as part of the names in the profiler.
 */
class EntropyBasedDropoutEffectiveIPFSolver(override val querySize: Int,
                                            override val solverName: String = "Entropy-Based Dropout Effective IPF")
  extends EffectiveIPFSolver(querySize, solverName) {
  /**
   * Select clusters to drop out.
   * The clusters will be ranked according to its entropy relative to the maximum possible entropy at its dimensionality (- N * N log 1/N = -log 1/N),
   * in a decreasing order (since we would prefer low-entropy ones).
   * At each time, the method will simply throw out the cluster with the lowest "relative" entropy
   * but will ignore a cluster if it contains at least one variable that is not covered by other remaining clusters.
   * The dropout will stop when
   * (1) 95% of the clusters have already been dropped,
   * (2) the remaining "coverage", defined by the sum of the number of variables among all clusters, drops below |Q| * (|Q| - 1)
   *     - this threshold can be adjusted—right now it simply comes from the fact that
   *       if we have all possible (|Q|-1)-dimensional cuboids, this "coverage" will be (|Q|-1) * C(|Q|, |Q|-1) = |Q| * (|Q|-1)
   * TODO: Some data structures to compute these things more efficiently
   */
  def selectiveDropout(): Unit = {
    val clustersQueue = mutable.Queue[Cluster]()
    clustersQueue ++= clusters.sortBy(cluster => (
      -/* high to low */ entropy(cluster.distribution) / (-math.log(1 / (1 << cluster.numVariables))) /* relative to maximum possible entropy */,
      cluster.numVariables
    )) // from high entropy to low entropy; low to high dimensionality in case of tie
    var numDroppedClusters: Int = 0
    val totalNumClusters = clusters.length
    var coverage: Int = clusters.map(_.numVariables).sum
    breakable { while (clustersQueue.nonEmpty) {
      if (numDroppedClusters >= totalNumClusters * 0.95) {
        break
      }
      val cluster = clustersQueue.dequeue()
      if (BitUtils.IntToSet(cluster.variables).forall(variable => (clusters.toSet - cluster).exists(cluster => isVariablesContained(1 << variable, cluster.variables)))) {
        if (coverage - cluster.numVariables < querySize * (querySize - 1)) {
          break
        }
        coverage -= cluster.numVariables
        numDroppedClusters += 1
        clusters = clusters.filter(_ != cluster)
      }
    } }
    println(s"\t\t\tDropped $numDroppedClusters cuboids")
  }
}
