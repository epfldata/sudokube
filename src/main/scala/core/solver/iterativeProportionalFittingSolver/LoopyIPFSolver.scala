package core.solver.iterativeProportionalFittingSolver

/**
 * The IPF with loopy scaling updates, i.e., with junction graphs allowing loops.
 * @author Zhekai Jiang
 * @param querySize Total number of dimensions queried.
 * @param solverName The name of the solver, "Random Junction Graph IPF",
 *                   to be used in the messages to be printed to the console and as part of the names in the profiler.
 */
abstract class LoopyIPFSolver(override val querySize: Int, override val solverName: String = "Loopy IPF") extends GraphicalIPFSolver(querySize, solverName) {
  /**
   * Connect all pairs of cliques completely, then delete variables from separators until the connectedness condition is satisfied.
   */
  def constructSeparators(): Unit = {
    junctionGraph.connectAllCliquesCompletely()
    (0 until querySize).foreach(variable => junctionGraph.retainSpanningTreeForVariable(variable))
    junctionGraph.initializeSeparators()
    junctionGraph.deleteZeroSeparators()
  }
}
