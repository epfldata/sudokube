package core.solver.iterativeProportionalFittingSolver

import core.SolverTools.entropy
import core.solver.iterativeProportionalFittingSolver.IPFUtils.isVariablesContained
import util.{Bits, Profiler}

import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

/**
 * Variant of the effective IPF where we throw out some cuboids with high dimensions.
 * TODO: Maybe a generalization for the current two heuristics of dropout.
 * @author Zhekai Jiang
 * @param querySize Total number of dimensions queried.
 */
class EntropyDropoutEffectiveIPFSolver(override val querySize: Int) extends EffectiveIPFSolver(querySize) {
  /**
   * Add a new known marginal distribution as a cluster.
   * Create network graph for elimination (triangulation to generate clique tree).
   * @param marginalVariables Sequence of marginal variables.
   * @param marginalDistribution Marginal distribution as a one-dimensional array (values encoded as bits of 1 in index).
   */
  override def add(marginalVariables: Seq[Int], marginalDistribution: Array[Double]): Unit = {
    normalizationFactor = marginalDistribution.sum
    val cluster = Cluster(Bits.toInt(marginalVariables), marginalDistribution.map(_ / normalizationFactor))
    clusters = cluster :: clusters
  }

  /**
   * Solve for the total distribution
   * TODO: confirm about convergence criteria (new idea – compare directly with previous distribution / compare to given marginal distributions?)
   * @return totalDistribution as a one-dimensional array (values encoded as bits of 1 in index).
   */
  override def solve(): Array[Double] = {
    selectiveDropout()

    clusters.foreach(cluster => graphicalModel.connectNodesCompletely(Bits.fromInt(cluster.variables).map(graphicalModel.nodes(_)).toSet, cluster))

    Profiler("Entropy-Based Dropout Effective IPF Junction Tree Construction") {
      constructJunctionTree()
    }

    junctionGraph.printAllCliquesAndSeparators()

    val totalNumUpdates = junctionGraph.cliques.foldLeft(0)((acc, clique) => acc + clique.N * clique.clusters.size)
    println(s"\t\t\tEntropy-Based Dropout Effective IPF number of updates per iteration (sum of |C|*2^|alpha| across all cliques): $totalNumUpdates")

    var totalDelta: Double = 0
    var numIterations: Int = 0
    Profiler("Entropy-Based Dropout Effective IPF Iterations") {
      do {
        numIterations += 1
        println(s"\t\t\tEntropy-Based Dropout Effective IPF Iteration $numIterations")
        totalDelta = iterativeUpdate()
      } while (totalDelta >= convergenceThreshold * totalNumUpdates)
    }
    println(s"\t\t\tEntropy-Based Dropout Effective IPF number of iterations $numIterations")

    Profiler("Entropy-Based Dropout Effective IPF Solution Derivation") {
      getTotalDistribution
      getSolution
    }

    solution
  }

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
      if (Bits.fromInt(cluster.variables).forall(variable => (clusters.toSet - cluster).exists(cluster => isVariablesContained(1 << variable, cluster.variables)))) {
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
