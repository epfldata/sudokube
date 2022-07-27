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
 * @param solverName The name of the solver, "Random Junction Graph IPF",
 *                   to be used in the messages to be printed to the console and as part of the names in the profiler.
 */
class RandomJunctionGraphIPFSolver(override val querySize: Int, override val solverName: String = "Random Junction Graph IPF")
  extends LoopyIPFSolver(querySize, solverName) {

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
  override def add(marginalVariables: Int, marginalDistribution: Array[Double]): Cluster = {
    val cluster = super.add(marginalVariables, marginalDistribution)
    val clique = Random.shuffle(junctionGraph.cliques.filter(clique => existsIntersectingVariables(clique.variables, marginalVariables)).toList).head
      // Randomly select one clique that has at least one intersecting variable to add the cluster to.
    clique.clusters += cluster
    clique.variables |= marginalVariables
    cluster
  }

  /**
   * Obtain the total distribution
   *
   * @return totalDistribution as a one-dimensional array (values encoded as bits of 1 in index).
   */
  override def solve(): Array[Double] = {
    Profiler(s"$solverName Junction Graph Construction") {
      junctionGraph.cliques.retain(_.clusters.nonEmpty)
      junctionGraph.deleteNonMaximalCliques()
      junctionGraph.initializeCliques()
      constructSeparators()
      junctionGraph.printAllCliquesAndSeparators()
    }

    solveWithJunctionGraph()
  }
}
