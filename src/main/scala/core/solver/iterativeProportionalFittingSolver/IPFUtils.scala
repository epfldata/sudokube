package core.solver.iterativeProportionalFittingSolver

import util.BitUtils

/**
 * Some helper methods useful for IPF solvers.
 * @author Zhekai Jiang
 */
object IPFUtils {
  /**
   * Get the number of 1s in the binary representation of a number.
   * Useful for getting the number of (marginal) variables.
   * @param n The number.
   * @return The number of 1s in the binary representation of n.
   */
  def getNumOnesInBinary(n: Int): Int = {
    var numOnes: Int = 0
    var tmp = n
    while (tmp != 0) {
      if (tmp % 2 == 1) {
        numOnes += 1
      }
      tmp /= 2
    }
    numOnes
  }

  /**
   * Proportionally update the total distribution based on a given marginal distribution, in the way of standard iterative proportional fitting.
   * @param totalNumVariables The number of variables in the total distribution.
   * @param totalDistribution The total distribution, indexed by a single integer with bits representing the values.
   * @param marginalVariables The variables (indices) involved in the marginal distribution, represented as bits of 1 in binary.
   * @param expectedMarginalDistribution The given marginal distribution, indexed by a single integer with bits representing the values.
   * @return The total delta of the entire update.
   */
  def updateTotalDistributionBasedOnMarginalDistribution(totalNumVariables: Int, totalDistribution: Array[Double],
                                                         marginalVariables: Int,
                                                         expectedMarginalDistribution: Array[Double]): Double = {
    var totalDelta: Double = 0.0

    val numMarginalVariables = getNumOnesInBinary(marginalVariables)
    (0 until 1 << numMarginalVariables).foreach(marginalVariablesValues => { // all possible values of the marginal variables
      val currentMarginalProbability = getMarginalProbability(totalNumVariables, totalDistribution, marginalVariables, marginalVariablesValues)
      totalDelta += updateDistributionForOneMarginalValue(
        totalNumVariables, totalDistribution,
        marginalVariables, marginalVariablesValues,
        if (currentMarginalProbability.abs < 1e-9)
          0
          // Avoids division by 0
          // If the marginal probability is 0, it must come from a 0 entry in the given marginal distribution
          // Simply set to 0 (cannot sum to 0 marginal probability if any entry in total distribution is non-zero)
        else
          expectedMarginalDistribution(marginalVariablesValues) / currentMarginalProbability
      )
    })

    totalDelta
  }

  /**
   * Obtain the marginal distribution of certain variables based on the total distribution.
   * The array marginalDistribution will be modified to contain the marginal probabilities.
   * @param totalNumVariables The number of variables in the total distribution.
   * @param totalDistribution The total distribution, indexed by a single integers with bits representing the values.
   * @param marginalVariables The marginal variables (indices) needed, represented as bits of 1 in binary.
   * @param marginalDistribution The array to store the marginal distribution. It will be modified to contain the marginal probabilities calculated.
   * @return The marginal distribution, which is the modified array marginalDistribution.
   */
  def getMarginalDistributionFromTotalDistribution(totalNumVariables: Int, totalDistribution: Array[Double],
                                                   marginalVariables: Int, marginalDistribution: Array[Double] = null): Array[Double] = {
    val numMarginalVariables = getNumOnesInBinary(marginalVariables)
    val numNonMarginalVariables = totalNumVariables - numMarginalVariables
    val marginalDistributionResultBuffer = if (marginalDistribution == null) Array.fill(1 << numMarginalVariables)(0.0) else marginalDistribution

    (0 until 1 << numMarginalVariables).foreach(marginalVariablesValues => {
      marginalDistributionResultBuffer(marginalVariablesValues) =
        (0 until 1 << numNonMarginalVariables).map(nonMarginalVariablesValues => {
          val allVariablesValues = encodeAllVariablesValues(totalNumVariables, marginalVariables, marginalVariablesValues, nonMarginalVariablesValues)
          totalDistribution(allVariablesValues)
        }).sum // sum out all non-marginal variables
    })
    marginalDistributionResultBuffer
  }

  /**
   * Scaling update for the total distribution based on one instance of values of the marginal variables.
   * @param totalNumVariables The number of variables in the total distribution.
   * @param totalDistribution The total distribution, indexed by a single integer with bits representing variable values.
   * @param marginalVariables Marginal variables, encoded as bits of 1 in an integer.
   * @param marginalVariablesValues Values of marginal variables, encoded as bits of 0/1 in an integer.
   * @param factor Scaling factor for update, i.e., $\frac{\hat{p_\alpha}(x_\alpha)}{P(x_\alpha)}$.
   * @return Total absolute change of probabilities, summed over all updated values in this update.
   */
  def updateDistributionForOneMarginalValue(totalNumVariables: Int, totalDistribution: Array[Double],
                                            marginalVariables: Int, marginalVariablesValues: Int, factor: Double): Double = {
    var totalDelta: Double = 0
    val numMarginalVariables = getNumOnesInBinary(marginalVariables)
    val numNonMarginalVariables = totalNumVariables - numMarginalVariables
    (0 until 1 << numNonMarginalVariables).foreach(nonMarginalVariablesValues => {
      val allVariablesValues: Int = encodeAllVariablesValues(totalNumVariables, marginalVariables, marginalVariablesValues, nonMarginalVariablesValues)
      val updatedProbability = totalDistribution(allVariablesValues) * factor
      totalDelta += (updatedProbability - totalDistribution(allVariablesValues)).abs
      totalDistribution(allVariablesValues) = updatedProbability
    })
    totalDelta
  }

  /**
   * Combine marginal & non-marginal variables' values in the original order of variables (for total distribution).
   * @param totalNumVariables Total number of variables in the total distribution.
   * @param marginalVariables Marginal variables, encoded as bits of 1 in an integer.
   * @param marginalVariablesValues Values of marginal variables, encoded as bits of 0/1 in an integer.
   * @param nonMarginalVariablesValues Values of non-marginal variables, encoded as bits of 0/1 in an integer.
   * @return The combined values of all variables, in the original order of variables (for total distribution).
   */
  def encodeAllVariablesValues(totalNumVariables: Int, marginalVariables: Int,
                               marginalVariablesValues: Int, nonMarginalVariablesValues: Int): Int = {
    val nonMarginalVariables = ((1 << totalNumVariables) - 1) ^ marginalVariables
    BitUtils.unprojectIntWithInt(marginalVariablesValues, marginalVariables) | BitUtils.unprojectIntWithInt(nonMarginalVariablesValues, nonMarginalVariables)
  }


  /**
   * Get the marginal probability of the specified values of the specified variables.
   * @param totalNumVariables The total number of variables in the total distribution.
   * @param totalDistribution The total distribution, indexed by a single integer with bits representing the variables' values.
   * @param marginalVariables Marginal variables, encoded as bits of 1 in an integer.
   * @param marginalVariablesValues Values of the variables, encoded as bits of 0/1 in an integer.
   * @return The marginal probability.
   */
  def getMarginalProbability(totalNumVariables: Int, totalDistribution: Array[Double],
                             marginalVariables: Int, marginalVariablesValues: Int): Double = {
    val numMarginalVariables = getNumOnesInBinary(marginalVariables)
    val numNonMarginalVariables = totalNumVariables - numMarginalVariables

    // Sum probabilities with all possible values of non-marginal variables.
    (0 until 1 << numNonMarginalVariables).map(nonMarginalVariablesValues => {
      val allVariablesValues: Int = encodeAllVariablesValues(totalNumVariables, marginalVariables, marginalVariablesValues, nonMarginalVariablesValues)
      totalDistribution(allVariablesValues)
    }).sum
  }

  /**
   * Get the marginal distribution of the specified values based on the total distribution.
   * @param totalNumVariables The total number of variables in the total distribution.
   * @param totalDistribution The total distribution, indexed by a single integer with bits representing the variables' values.
   * @param numMarginalVariables Marginal variables, encoded as bits of 1 in an integer.
   * @param marginalVariables Values of the variables, encoded as bits of 0/1 in an integer.
   * @return The marginal distribution, indexed by a single integer with bits representing variables' values.
   */
  def getMarginalDistribution(totalNumVariables: Int, totalDistribution: Array[Double],
                              numMarginalVariables: Int, marginalVariables: Int): Array[Double] = {
    val marginalDistribution: Array[Double] = Array.fill(1 << numMarginalVariables)(0)
    marginalDistribution.indices.foreach(marginalVariablesValues => marginalDistribution(marginalVariablesValues) =
      getMarginalProbability(totalNumVariables, totalDistribution, marginalVariables, marginalVariablesValues))
    marginalDistribution
  }

  /**
   * Get the "local" indices of the variables within a subset of variables.
   * e.g. clique variables 0, 2, 4, 8; cluster variables 2, 4 => indices of cluster variables within clique are 1, 2
   * @param variables The "global" indices of the variables needed, encoded as bits of 1 in a single integer.
   * @param variablesInSubset The "global" indices of the variables in the subset, encoded as bits of 1 in a single integer.
   * @return The "local" indices of the variables within the subset, encoded as bits of 1 in a single integer.
   */
  def getVariableIndicesWithinVariableSubset(variables: Int, variablesInSubset: Int): Int = {
    var tmpVariables = variables
    var tmpVariablesInSubset = variablesInSubset
    var variableIndicesWithinSubset: Int = 0
    var mask: Int = 1
    while (tmpVariablesInSubset != 0) {
      if ((tmpVariablesInSubset & 1) == 1) { // current variable is present in the subset
        if ((tmpVariables & 1) == 1) { // current variable is needed
          variableIndicesWithinSubset |= mask // add a bit of one in the corresponding position
        }
        mask <<= 1
      }
      tmpVariables >>= 1
      tmpVariablesInSubset >>= 1
    }
    variableIndicesWithinSubset
  }

  /**
   * Test whether a set of variables are fully contained in another set of variables.
   * Subset tested using intersections, given by bitwise AND.
   * @param containedVariables The variables that are presumably contained, encoded as bits of 1 in a single integer.
   * @param containingVariables The variables that are presumably containing the contained variables, encoded as bits of 1 in a single integer.
   * @return Whether the first set of variables is fully contained in the second set of variables.
   */
  def isVariablesContained(containedVariables: Int, containingVariables: Int): Boolean = {
    (containedVariables & containingVariables) == containedVariables
  }

  def existsIntersectingVariables(variables1: Int, variables2: Int): Boolean = {
    (variables1 & variables2) != 0
  }
}
