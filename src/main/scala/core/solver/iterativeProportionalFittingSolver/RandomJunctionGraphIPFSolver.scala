package core.solver.iterativeProportionalFittingSolver

import core.solver.iterativeProportionalFittingSolver.IPFUtils.existsIntersectingVariables
import core.solver.iterativeProportionalFittingSolver.JunctionGraph.Clique
import util.Profiler

import scala.util.Random

/**
 * A method similar to bucket elimination,
 * where clusters are added randomly to one of the buckets (cliques) that contains some intersecting variables,
 * and separators are constructed using a method similar to the worst loopy IPF
 * (connecting completely and delete variables from separators until "connectedness condition" is satisfied for all variable.)
 * @author Zhekai Jiang
 * @param querySize Total number of dimensions queried.
 */
class RandomJunctionGraphIPFSolver(override val querySize: Int) extends LoopyIPFSolver(querySize) {
  // Similar to bucket elimination, every variable will be placed in a separate "bucket" initially.
  (0 until querySize).foreach(variable => junctionGraph.cliques += new Clique(1 << variable))

  /**
   * Add a new known marginal distribution as a cluster.
   * Among all cliques that contain at least one of the variables in the cluster,
   * choose a random one to add the cluster into, and update the label so that it covers the new cluster.
   * Cliques (distributions) need to be reinitialized after this as the labels keep changing.
   *
   * @param marginalVariables    Sequence of marginal variables.
   * @param marginalDistribution Marginal distribution as a one-dimensional array (values encoded as bits of 1 in index).
   */
  override def add(marginalVariables: Int, marginalDistribution: Array[Double]): Unit = {
    normalizationFactor = marginalDistribution.sum
    val clusterVariables = marginalVariables
    val cluster = Cluster(clusterVariables, marginalDistribution.map(_ / normalizationFactor))
    clusters = cluster :: clusters
    val clique = Random.shuffle(junctionGraph.cliques.filter(clique => existsIntersectingVariables(clique.variables, clusterVariables)).toList).head
    clique.clusters += cluster
    clique.variables |= clusterVariables
  }

  /**
   * Obtain the total distribution
   *
   * @return totalDistribution as a one-dimensional array (values encoded as bits of 1 in index).
   */
  override def solve(): Array[Double] = {
    Profiler("Random Junction Graph IPF Junction Graph Construction") {
      junctionGraph.cliques.retain(_.clusters.nonEmpty)
      junctionGraph.deleteNonMaximalCliques()
      junctionGraph.initializeCliques()
      constructSeparators()
      junctionGraph.printAllCliquesAndSeparators()
    }

    var totalDelta: Double = 0
    var numIterations: Int = 0

    val totalNumUpdates = junctionGraph.cliques.foldLeft(0)((acc, clique) => acc + clique.N * clique.clusters.size)
    println(s"\t\t\tRandom Junction Graph IPF number of updates per iteration (sum of |C|*2^|alpha| across all cliques): $totalNumUpdates")

    Profiler("Random Junction Graph IPF Iterations") {
      do {
        numIterations += 1
        println(s"\t\t\tRandom Junction Graph IPF Iteration $numIterations")
        totalDelta = iterativeUpdate() // Start with any clique
        println(s"\t\t\tTotal delta: $totalDelta, threshold = ${convergenceThreshold * totalNumUpdates}")
      } while (totalDelta >= convergenceThreshold * totalNumUpdates)
    }
    println(s"\t\t\tRandom Junction Graph IPF number of iterations $numIterations")

    Profiler("Random Junction Graph IPF Solution Derivation") {
      getTotalDistribution
      getSolution
    }
  }
}
