package core.solver.iterativeProportionalFittingSolver

import core.SolverTools.entropy
import core.solver.iterativeProportionalFittingSolver.IPFUtils.{getNumOnesInBinary, isVariablesContained}
import util.{BitUtils, Profiler}

import scala.util.control.Breaks.{break, breakable}

/**
 * Variant of the effective IPF where we simply throw out some low-dimensional cuboids.
 * TODO: Maybe a generalization for the current two heuristics of dropout.
 * @author Zhekai Jiang
 * @param querySize Total number of dimensions queried.
 */
class DropoutEffectiveIPFSolver(override val querySize: Int) extends EffectiveIPFSolver(querySize) {
  /**
   * Add a new known marginal distribution as a cluster.
   * Create network graph for elimination (triangulation to generate clique tree).
   * @param marginalVariables Sequence of marginal variables.
   * @param marginalDistribution Marginal distribution as a one-dimensional array (values encoded as bits of 1 in index).
   */
  override def add(marginalVariables: Seq[Int], marginalDistribution: Array[Double]): Unit = {
    normalizationFactor = marginalDistribution.sum
    val cluster = Cluster(BitUtils.SetToInt(marginalVariables), marginalDistribution.map(_ / normalizationFactor))
    clusters = cluster :: clusters
  }

  /**
   * Solve for the total distribution
   * TODO: confirm about convergence criteria (new idea – compare directly with previous distribution / compare to given marginal distributions?)
   * @return totalDistribution as a one-dimensional array (values encoded as bits of 1 in index).
   */
  override def solve(): Array[Double] = {
    selectiveDropout()

    clusters.foreach(cluster => graphicalModel.connectNodesCompletely(BitUtils.IntToSet(cluster.variables).map(graphicalModel.nodes(_)).toSet, cluster))

    Profiler("Dropout Effective IPF Junction Tree Construction") {
      constructJunctionTree()
    }

    junctionGraph.printAllCliquesAndSeparators()

    val totalNumUpdates = junctionGraph.cliques.foldLeft(0)((acc, clique) => acc + clique.N * clique.clusters.size)
    println(s"\t\t\tDropout Effective IPF number of updates per iteration (sum of |C|*2^|alpha| across all cliques): $totalNumUpdates")

    var totalDelta: Double = 0
    var numIterations: Int = 0
    Profiler("Dropout Effective IPF Iterations") {
      do {
        numIterations += 1
        println(s"\t\t\tDropout Effective IPF Iteration $numIterations")
        totalDelta = iterativeUpdate()
      } while (totalDelta >= convergenceThreshold * totalNumUpdates)
    }
    println(s"\t\t\tDropout Effective IPF number of iterations $numIterations")

    Profiler("Dropout Effective IPF Solution Derivation") {
      getTotalDistribution
      getSolution
    }

    solution
  }

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
