package core.solver.iterativeProportionalFittingSolver

import util.{Bits, Profiler}

/**
 * Effective iterative proportional fitting, based on junction tree (or clique tree).
 * TODO: Confirm about the uses of collection data structures (Set, Map, Seq)
 * @author Zhekai Jiang
 * @param querySize Total number of dimensions queried.
 */
class EffectiveIPFSolver(override val querySize: Int) extends GraphicalIPFSolver(querySize) {

  /**
   * Add a new known marginal distribution as a cluster.
   * Create network graph for elimination (triangulation to generate clique tree).
   * @param marginalVariables Sequence of marginal variables.
   * @param marginalDistribution Marginal distribution as a one-dimensional array (values encoded as bits of 1 in index).
   */
  def add(marginalVariables: Seq[Int], marginalDistribution: Array[Double]): Unit = {
    normalizationFactor = marginalDistribution.sum
    val cluster = Cluster(Bits.toInt(marginalVariables), marginalDistribution.map(_ / normalizationFactor))
    clusters = cluster :: clusters
    graphicalModel.connectNodesCompletely(marginalVariables.map(graphicalModel.nodes(_)).toSet, cluster)
  }

  /**
   * Solve for the total distribution
   * TODO: confirm about convergence criteria (new idea – compare directly with previous distribution / compare to given marginal distributions?)
   * @return totalDistribution as a one-dimensional array (values encoded as bits of 1 in index).
   */
  def solve(): Array[Double] = {
    Profiler("Effective IPF Junction Tree Construction") {
      constructJunctionTree()
    }

    println(s"\t\t\t${junctionGraph.cliques.size} cliques, ${junctionGraph.separators.size} separators")
    println("\t\t\tCliques:")
    junctionGraph.cliques.foreach(clique =>
      println(s"\t\t\t\t${clique.numVariables} variables: ${Bits.fromInt(clique.variables).mkString(":")}, "
        + s"${clique.clusters.size} clusters: ${clique.clusters.map(cluster => Bits.fromInt(cluster.variables).mkString(":")).mkString(", ")}")
    )
    val totalNumUpdates = junctionGraph.cliques.foldLeft(0)((acc, clique) => acc + clique.N * clique.clusters.size)
    println(s"\t\t\tEffective IPF number of updates per iteration (sum of |C|*2^|alpha| across all cliques): $totalNumUpdates")

    var totalDelta: Double = 0
    var numIterations: Int = 0
    Profiler("Effective IPF Iterations") {
      do {
        numIterations += 1
        totalDelta = iterativeUpdate()
      } while (totalDelta >= convergenceThreshold * totalNumUpdates)
    }
    println(s"\t\t\tEffective IPF number of iterations $numIterations")

    Profiler("Effective IPF Solution Derivation") {
      getTotalDistribution
      getSolution
    }

    solution
  }

  /**
   * Construct the junction tree:
   * - triangulate and construct cliques using node elimination
   * - connect all pairs of cliques to get a complete graph (also delete non-maximal cliques at the same time)
   * - obtain maximum spanning tree as junction tree
   */
  def constructJunctionTree(): Unit = {
    junctionGraph.constructCliquesFromGraph(graphicalModel)
    junctionGraph.deleteNonMaximalCliques()
    junctionGraph.connectAllCliquesCompletely()
    junctionGraph.constructMaximumSpanningTree()
  }
}
