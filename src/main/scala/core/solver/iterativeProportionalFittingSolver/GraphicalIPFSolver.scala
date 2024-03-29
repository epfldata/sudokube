package core.solver.iterativeProportionalFittingSolver

import util.{BitUtils, Profiler}

import scala.collection.mutable

/**
 * Generic definition of IPF solvers based on graphical models.
 * Includes definitions a graphical model and a junction graph.
 * @author Zhekai Jiang
 * @param querySize Total number of dimensions queried.
 */
abstract class GraphicalIPFSolver(override val querySize: Int) extends IPFSolver(querySize) {

  val graphicalModel = new IPFGraphicalModel(querySize)
  val junctionGraph = new JunctionGraph()

  /**
   * Calculate the total distribution based on the marginal distributions in cliques and separators.
   * totalDistribution will be updated and returned.
   * TODO: Look at floating point precisions?
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
      totalDelta += dfsUpdate(remainingCliques.head, remainingCliques, visitedSeparators)
    }
    totalDelta
  }

  /**
   * Scaling and propagation update in DFS order.
   * @param clique The current clique.
   * @param visitedSeparators The set of all separators already visited on the path.
   * @return The total delta during the updates starting from the current clique.
   */
  def dfsUpdate(clique: JunctionGraph.Clique, remainingCliques: mutable.Set[JunctionGraph.Clique], visitedSeparators: mutable.Set[JunctionGraph.Separator]): Double = {
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

    Profiler("Effective IPF Update Clique") {
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
    Profiler("Effective IPF Update Separator") {
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
    Profiler("Effective IPF Update Clique") {
      IPFUtils.updateTotalDistributionBasedOnMarginalDistribution(
        clique.numVariables,
        clique.distribution,
        IPFUtils.getVariableIndicesWithinVariableSubset(separator.variables, clique.variables),
        separator.distribution
      )
    }
  }

}
