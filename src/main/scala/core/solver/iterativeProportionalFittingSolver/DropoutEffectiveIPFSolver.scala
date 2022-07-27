package core.solver.iterativeProportionalFittingSolver

import util.Profiler

/**
 * Variants of the effective IPF where we throw out some cuboids.
 * The strategy of dropout has to be implemented in selectiveDropout() in concrete subclasses.
 *
 * @author Zhekai Jiang
 * @param querySize Total number of dimensions queried.
 * @param solverName The name of the concrete method (e.g. "Dimension-Based Dropout Effective IPF"), "Dropout Effective IPF" by default,
 *                   to be used in the messages to be printed to the console and as part of the names in the profiler.
 */
abstract class DropoutEffectiveIPFSolver(override val querySize: Int,
                                         override val solverName: String = "Dropout Effective IPF") extends EffectiveIPFSolver(querySize, solverName) {

  /**
   * Solve for the total distribution.
   * @return Solution as a one-dimensional array (values encoded as bits of 1 in index).
   */
  override def solve(): Array[Double] = {
    Profiler(s"$solverName Cuboid Dropout") {
      selectiveDropout()
    }
    super.solve()
  }

  /**
   * Select clusters to drop out. The strategy has to be implemented in selectiveDropout() in concrete subclasses.
   */
  def selectiveDropout(): Unit
}
