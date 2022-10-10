package core.solver.iterativeProportionalFittingSolver

import util.{BitUtils, Profiler}

import scala.collection.mutable

/**
 * Generic definition of IPF solvers based on graphical models.
 * Includes definitions a graphical model and a junction graph.
 * @author Zhekai Jiang
 * @param querySize Total number of dimensions queried.
 * @param solverName The name of the concrete method (e.g. "Effective IPF", "Loopy IPF"), empty by default,
 *                   to be used in the messages to be printed to the console and as part of the names in the profiler.
 */
abstract class GraphicalIPFSolver(override val querySize: Int, override val solverName: String) extends IPFSolver(querySize, solverName) {

  val graphicalModel = new IPFGraphicalModel(querySize)
  val junctionGraph = new JunctionGraph()
  var numIterations: Int = 0
  /**
   * Create graphical model for elimination.
   */
  def constructGraphicalModel(): Unit = {
    clusters.foreach(cluster =>
      graphicalModel.connectNodesCompletely(BitUtils.IntToSet(cluster.variables).map(graphicalModel.nodes(_)).toSet, cluster)
    )
  }

  /**
   * After constructing the junction graph/tree, perform iterative updates.
   */
  def solveWithJunctionGraph(): Array[Double] = {
    val totalNumUpdates: Long = junctionGraph.cliques.foldLeft(0L)((acc, clique) => acc + 1L * clique.N * clique.clusters.size)
    //println(s"\t\t\t$solverName number of entries (sum of |C|*2^|alpha| across all cliques): $totalNumUpdates")

    var totalDelta: Double = 0
    numIterations = 0
    Profiler(s"$solverName Iterations") {
      do {
        numIterations += 1
        //println(s"\t\t\t$solverName Iteration $numIterations")
        totalDelta = iterativeUpdate()
      } while (totalDelta > Math.max(convergenceThreshold * totalNumUpdates, 1e-5) /* in case of 0 clusters */)
        // There may be alternative ways to define the convergence criteria,
        // e.g. compare directly with previous distribution, compare to given marginal distributions, max of all deltas, etc.
    }
    //println(s"\t\t\t$solverName number of iterations $numIterations")

    Profiler(s"$solverName Solution Derivation") {
      getTotalDistribution
      getSolution
    }

    solution
  }

  /**
   * Calculate the total distribution based on the marginal distributions in cliques and separators.
   * totalDistribution will be updated and returned.
   * @return The array containing the total distribution, indexed by an integer with variable values encoded as bits of 0 or 1.
   */
  def getTotalDistribution: Array[Double] = {
    (0 until 1 << querySize).foreach(allVariablesValues => {
      val cliquesProduct = junctionGraph.cliques.foldLeft(1.0)((acc, clique) => {
        acc * clique.distribution(BitUtils.projectIntWithInt(allVariablesValues, clique.variables))
      })
      val separatorsProduct = junctionGraph.separators.foldLeft(1.0)((acc, separator) => {
        acc * separator.distribution(BitUtils.projectIntWithInt(allVariablesValues, separator.variables))
      })
      totalDistribution(allVariablesValues) = if (separatorsProduct.abs == 0) 0 else cliquesProduct / separatorsProduct
    })
    totalDistribution
  }

  /**
   * One iteration of updates, in DFS order (going through all cliques to update with all clusters).
   * @return
   */
  def iterativeUpdate(): Double = {
    val remainingCliques: mutable.Set[JunctionGraph.Clique] = junctionGraph.cliques.clone()
    val visitedSeparators: mutable.Set[JunctionGraph.Separator] = mutable.Set[JunctionGraph.Separator]()
    var totalDelta: Double = 0.0
    while (remainingCliques.nonEmpty) { // Could be a forest?
      totalDelta += dfsUpdate(
        remainingCliques.minBy(clique => clique.clusters.size << clique.numVariables),
          // Start with any clique as the root.
          // The fluctuation in runtime turned out to be caused by imbalanced cliques.
          // So here we simply pick the one that has the least number of entries to update to reduce the runtime since the root will be updated multiple times with backtracking.
        remainingCliques, visitedSeparators)
    }
    totalDelta
  }

  /**
   * Scaling and propagation update in DFS order.
   * @param clique The current clique.
   * @param visitedSeparators The set of all separators already visited on the path.
   * @return The total delta during the updates starting from the current clique.
   */
  def dfsUpdate(clique: JunctionGraph.Clique, remainingCliques: mutable.Set[JunctionGraph.Clique],
                visitedSeparators: mutable.Set[JunctionGraph.Separator]): Double = {
    remainingCliques -= clique

    var totalDelta: Double = 0.0

    totalDelta += updateCliqueBasedOnClusters(clique)

    clique.adjacencyList.foreach {
      case (nextClique, separator) if !visitedSeparators.contains(separator) =>
        visitedSeparators += separator
        updateSeparatorBasedOnClique(separator, clique)
        totalDelta += updateCliqueBasedOnSeparator(nextClique, separator)
        totalDelta += dfsUpdate(nextClique, remainingCliques, visitedSeparators)
        updateSeparatorBasedOnClique(separator, nextClique)
        totalDelta += updateCliqueBasedOnSeparator(clique, separator)
        totalDelta += updateCliqueBasedOnClusters(clique)
      case _ => ()
    }

    totalDelta
  }

  /**
   * Iterative scaling update of one clique using all clusters associated with it.
   * @param clique The clique to be updated.
   * @return Total delta of the update.
   */
  def updateCliqueBasedOnClusters(clique: JunctionGraph.Clique): Double = {
    var totalDelta: Double = 0.0

    Profiler(s"$solverName Update Clique Based on Clusters") {
      clique.clusters.foreach(cluster => {
        totalDelta += IPFUtils.updateTotalDistributionBasedOnMarginalDistribution(
          clique.numVariables,
          clique.distribution,
          IPFUtils.getVariableIndicesWithinVariableSubset(cluster.variables, clique.variables),
          cluster.distribution
        )
      })
    }

    totalDelta
  }

  /**
   * Update the distribution in the separator based on the distribution of the clique.
   * @param separator The separator to be updated.
   * @param clique The given clique.
   */
  def updateSeparatorBasedOnClique(separator: JunctionGraph.Separator, clique: JunctionGraph.Clique): Unit = {
    Profiler(s"$solverName Update Separator") {
      IPFUtils.getMarginalDistributionFromTotalDistribution(
        clique.numVariables,
        clique.distribution,
        IPFUtils.getVariableIndicesWithinVariableSubset(separator.variables, clique.variables),
        separator.distribution
      )
    }
  }

  /**
   * Scaling update of the clique based on the marginal distribution in the separator.
   * @param clique The clique to be updated.
   * @param separator The given separator.
   * @return The total delta of the update.
   */
  def updateCliqueBasedOnSeparator(clique: JunctionGraph.Clique, separator: JunctionGraph.Separator): Double = {
    Profiler(s"$solverName Update Clique Based on Separator") {
      IPFUtils.updateTotalDistributionBasedOnMarginalDistribution(
        clique.numVariables,
        clique.distribution,
        IPFUtils.getVariableIndicesWithinVariableSubset(separator.variables, clique.variables),
        separator.distribution
      )
    }
  }
}
