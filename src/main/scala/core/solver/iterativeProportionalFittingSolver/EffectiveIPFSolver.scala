package core.solver.iterativeProportionalFittingSolver

import util.Profiler

/**
 * Effective iterative proportional fitting, based on junction tree (or clique tree).
 * @author Zhekai Jiang
 * @param querySize Total number of dimensions queried.
 * @param solverName The name of the concrete method (e.g. "Effective IPF", "Entropy-Based Dropout Effective IPF"), "EffectiveIPF" by default,
 *                   to be used in the messages to be printed to the console and as part of the names in the profiler.
 */
class EffectiveIPFSolver(override val querySize: Int, override val solverName: String = "Effective IPF") extends GraphicalIPFSolver(querySize, solverName) {

  /**
   * Solve for the total distribution.
   * @return Solution (un-normalized) as a one-dimensional array (values encoded as bits of 1 in index).
   */
  def solve(): Array[Double] = {
    Profiler(s"$solverName Graphical Model Construction") {
      constructGraphicalModel()
    }
    Profiler(s"$solverName Junction Tree Construction") {
      constructJunctionTree()
    }
    solveWithJunctionGraph()
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

    if (oneDimMarginals != null) {
      junctionGraph.fixOneDimensionalMarginals(oneDimMarginals)
    }

    junctionGraph.printAllCliquesAndSeparators()
  }
}
