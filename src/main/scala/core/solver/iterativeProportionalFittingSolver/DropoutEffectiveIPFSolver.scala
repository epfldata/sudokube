package core.solver.iterativeProportionalFittingSolver

import core.solver.iterativeProportionalFittingSolver.IPFUtils.getNumOnesInBinary
import util.{Bits, Profiler}

import scala.util.Random
import scala.util.control.Breaks.{break, breakable}

/**
 * Variant of the effective IPF where we simply throw out some low-dimensional cuboids.
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
    val cluster = Cluster(Bits.toInt(marginalVariables), marginalDistribution.map(_ / normalizationFactor))
    clusters = cluster :: clusters
  }

  /**
   * Solve for the total distribution
   * TODO: confirm about convergence criteria (new idea – compare directly with previous distribution / compare to given marginal distributions?)
   * @return totalDistribution as a one-dimensional array (values encoded as bits of 1 in index).
   */
  override def solve(): Array[Double] = {
    dropOutLowDimensionalClusters()

    clusters.foreach(cluster => graphicalModel.connectNodesCompletely(Bits.fromInt(cluster.variables).map(graphicalModel.nodes(_)).toSet, cluster))

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

  def dropOutLowDimensionalClusters(): Unit = {
    var numDroppedClusters: Int = 0
    val totalNumClusters = clusters.length
    val maxNumDimensions = clusters.map(cluster => getNumOnesInBinary(cluster.variables)).max
    val coverage: Int = clusters.map(_.numVariables).sum
    var reducedCoverage: Int = 0
    breakable { while (true) {
      if (numDroppedClusters >= totalNumClusters * 0.3) {
        break
      }
      val minNumDimensions = clusters.map(cluster => getNumOnesInBinary(cluster.variables)).min
      if (minNumDimensions * 2 > maxNumDimensions) {
        break
      }
      val droppedCluster = Random.shuffle(clusters.filter(cluster => getNumOnesInBinary(cluster.variables) == minNumDimensions)).head
      numDroppedClusters += 1
      reducedCoverage += droppedCluster.numVariables
      if (reducedCoverage >= coverage * 0.3 || coverage - reducedCoverage < querySize * 3) {
        break
      }
      println(s"\t\t\tDropping out ${Bits.fromInt(droppedCluster.variables).mkString(":")}")
      clusters = clusters.filter(_ != droppedCluster)
    } }
  }
}
