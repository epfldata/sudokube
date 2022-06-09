package core.solver.iterativeProportionalFittingSolver

import core.SolverTools.error
import util.{Bits, Profiler}

import java.io.PrintStream

/**
 * Vanilla version of linear proportional fitting.
 * @author Zhekai Jiang
 * @param numDimensionsQueried Total number of dimensions queried.
 */
class VanillaIPFSolver(val numDimensionsQueried: Int,
                       val isExperimenting: Boolean = false,
                       val trueDistribution: Array[Double] = null, /* for experimentation only */
                       val timeErrorFileOut: PrintStream = null, /* for experimentation only */
                       val cubeName: String = "", val query: String = "" /* for experimentation only */) {
  /* private */ val N: Int = 1 << numDimensionsQueried
  /* private */ var clusters: List[Cluster] = List[Cluster]()
  /* private */ var totalDistribution: Array[Double] = Array.fill(N)(1.0 / N) // Initialize to uniform
  var solution: Array[Double] = Array[Double]()
  def getSolution: Array[Double] = {
    solution = totalDistribution.map(_ * normalizationFactor)
    solution
  }
  /* private */ val convergenceThreshold: Double = 1e-5
  /* private */ var normalizationFactor: Double = 1.0

  /**
   * Add a new known marginal distribution as a cluster.
   * @param marginalVariables Sequence of marginal variables.
   * @param marginalDistribution Marginal distribution as a one-dimensional array (values encoded as bits of 1 in index).
   */
  def add(marginalVariables: Seq[Int], marginalDistribution: Array[Double]): Unit = {
    normalizationFactor = marginalDistribution.sum
    clusters = Cluster(Bits.toInt(marginalVariables), marginalDistribution.map(_ / normalizationFactor)) :: clusters
  }

  /**
   * Obtain the solution, un-normalized
   * @return solution as a one-dimensional array (values encoded as bits of 1 in index).
   */
  def solve(): Array[Double] = {
    var totalDelta: Double = 0.0
    var numIterations = 0

    if (isExperimenting)
      printExperimentTimeErrorDataToFile()

    do {
      numIterations += 1
      totalDelta = updateDistribution()

      if (isExperimenting) {
        printExperimentTimeErrorDataToFile()
        //      println(s"\t\tTotal delta: $totalDelta, threshold: ${convergenceThreshold * N * clusters.length * totalDistribution.sum}")
        //      println(s"\t\tError: ${error(naiveRes, totalDistribution)}")
      }

    } while (totalDelta >= convergenceThreshold * N * clusters.length * totalDistribution.sum)
    println(s"\t\tnumber of iterations: $numIterations, log N: $numDimensionsQueried")
    getSolution
  }

  /**
   * One iterative scaling update.
   * @return totalDelta, total absolute change of probabilities
   */
  /* private */ def updateDistribution(): Double = {
    var totalDelta: Double = 0.0

    for (Cluster(marginalVariables: Int, expectedMarginalDistribution: Array[Double]) <- clusters) {
      val numMarginalVariables = getNumOnesInBinary(marginalVariables)
      for (marginalVariablesValues: Int <- 0 until 1 << numMarginalVariables) { // all possible values of the marginal variables
        val currentMarginalProbability = getMarginalProbability(marginalVariables, marginalVariablesValues)
        totalDelta += updateDistributionForOneMarginalValue(
          marginalVariables,
          marginalVariablesValues,
          if (currentMarginalProbability.abs < 1e-9)
            0
              // Marginal probability is 0, must come from a 0 entry in the given marginal distribution
              // Simply set to 0 (cannot sum to 0 marginal probability if an entry is non-zero)
              // Avoids division by 0
          else
            expectedMarginalDistribution(marginalVariablesValues) / currentMarginalProbability
        )
      }
    }

    totalDelta
  }

  /**
   * Get the marginal probability of the specified values of the specified variables.
   *
   * @param marginalVariables Marginal variables, encoded as bits of 1 in an integer.
   * @param marginalVariablesValues Values of the variables, encoded as bits of 0/1 in an integer.
   * @return The marginal probability as a double.
   */
  /* private */ def getMarginalProbability(marginalVariables: Int, marginalVariablesValues: Int): Double = {
    var marginalProbability: Double = 0.0

    val numMarginalVariables = getNumOnesInBinary(marginalVariables)
    val numNonMarginalVariables = numDimensionsQueried - numMarginalVariables

    // Sum probabilities with all possible values of non-marginal variables.
    for (nonMarginalVariablesValues <- 0 until 1 << numNonMarginalVariables) {
      val allVariablesValues: Int = encodeAllVariablesValues(marginalVariables, marginalVariablesValues, nonMarginalVariablesValues)
      marginalProbability += totalDistribution(allVariablesValues)
    }

    marginalProbability
  }

  /**
   * Combine marginal & non-marginal variables' values in the original order of variables (for total distribution).
   *
   * @param marginalVariables Marginal variables, encoded as bits of 1 in an integer.
   * @param marginalVariablesValues Values of marginal variables, encoded as bits of 1 in an integer.
   * @param nonMarginalVariablesValues Values of non-marginal variables, encoded as bits of 0/1 in an integer.
   * @return The combined values of all variables, in the original order of variables (for total distribution).
   */
  /* private */ def encodeAllVariablesValues(marginalVariables: Int, marginalVariablesValues: Int, nonMarginalVariablesValues: Int): Int = {
    val nonMarginalVariables = ((1 << numDimensionsQueried) - 1) ^ marginalVariables
    Bits.unproject(marginalVariablesValues, marginalVariables) | Bits.unproject(nonMarginalVariablesValues, nonMarginalVariables)
  }

  /**
   * Scaling update for one instance of values of the marginal variables.
   *
   * @param marginalVariables Marginal variables, encoded as bits of 1 in an integer.
   * @param marginalVariablesValues Values of marginal variables, encoded as bits of 0/1 in an integer.
   * @param factor Scaling factor for update, i.e., $\frac{\hat{p_\alpha}(x_\alpha)}{P(x_\alpha)}$.
   * @return totalDelta, total absolute change of probabilities, summed over all updated values in this update.
   */
  /* private */ def updateDistributionForOneMarginalValue(marginalVariables: Int, marginalVariablesValues: Int, factor: Double): Double = {
    var totalDelta: Double = 0
    val numMarginalVariables = getNumOnesInBinary(marginalVariables)
    val numNonMarginalVariables = numDimensionsQueried - numMarginalVariables
    for (nonMarginalVariablesValues <- 0 until 1 << numNonMarginalVariables) {
      val allVariablesValues: Int = encodeAllVariablesValues(marginalVariables, marginalVariablesValues, nonMarginalVariablesValues)
      val updatedProbability = totalDistribution(allVariablesValues) * factor
      totalDelta += (updatedProbability - totalDistribution(allVariablesValues)).abs
      totalDistribution(allVariablesValues) = updatedProbability
    }
    totalDelta
  }

  /* private */ def getNumOnesInBinary(n: Int): Int = {
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

  private def printExperimentTimeErrorDataToFile(): Unit = {
    val currentTime = System.nanoTime()
    val totalTime = (currentTime - Profiler.startTimers("Vanilla IPF Total")) / 1000
    val solveTime = (currentTime - Profiler.startTimers("Vanilla IPF Solve")) / 1000
    val currentError = error(trueDistribution, getSolution)
    println(s"\t\tTotal Time: $totalTime, solveTime: $solveTime, Error: $currentError")
    // CubeName, Query, QSize, IPFTotalTime(us), IPFSolveTime(us), IPFErr
    timeErrorFileOut.println(s"$cubeName, $query, $numDimensionsQueried, $totalTime, $solveTime, $currentError")
  }
}
