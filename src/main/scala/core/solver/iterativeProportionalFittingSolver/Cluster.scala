package core.solver.iterativeProportionalFittingSolver

import core.solver.iterativeProportionalFittingSolver.IPFUtils.getNumOnesInBinary

/**
 * A Cluster, i.e., given knowledge of marginal distribution of a subset of variables.
 * May be useful for graphical models (nodes in junction tree, region graph, etc.)
 *
 * @author Zhekai Jiang
 * @param variables The indices (in the original solver) of the variables concerned, encoded as bits of 1 in an Int.
 * @param distribution The marginal distribution of the variables. Values are encoded as bits of 1 in the index.
 *                     Indices are now with respect to the marginal variables, not all variables in the original solver.
 *                     e.g. given marginal variables 3, 11, and 16, they will have indices 0, 1, and 2 for the marginal distribution.
 */
case class Cluster(variables: Int, distribution: Array[Double]) {
  def numVariables: Int = getNumOnesInBinary(variables)
}
