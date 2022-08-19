package core.solver.iterativeProportionalFittingSolver

import core.solver.SolverTools.error
import util.{BitUtils, Profiler}

import java.io.PrintStream

/**
 * Vanilla version of linear proportional fitting.
 * @author Zhekai Jiang
 * @param querySize Total number of dimensions queried.
 * @param printErrorForEachIteration Record error at the end of each iteration. (Note error checking can be slow.)
 * @param printErrorForEachUpdate Record error at the end of each update (fitting one cuboid). (Note error checking can be slow.)
 * @param trueResult The true result of the query.
 * @param timeErrorFileOut The PrintStream to output time and error.
 * @param cubeName Cube name.
 * @param query The query as a string.
 */
class VanillaIPFSolver(override val querySize: Int,
                       val printErrorForEachIteration: Boolean = false,
                       val printErrorForEachUpdate: Boolean = false,
                       val trueResult: Array[Double] = null, /* for experimentation only */
                       val timeErrorFileOut: PrintStream = null, /* for experimentation only */
                       val cubeName: String = "", val query: String = "" /* for experimentation only */)
  extends IPFSolver(querySize, "Vanilla IPF") {

  /**
   * Obtain the solution, un-normalized
   * @return solution as a one-dimensional array (values encoded as bits of 1 in index).
   */
  def solve(): Array[Double] = {
    println(s"\t\t\tVanilla IPF number of entries (|C| 2^|Q|): ${clusters.size << querySize}")

    var totalDelta: Double = 0.0
    var numIterations = 0

    if (printErrorForEachIteration || printErrorForEachUpdate)
      printExperimentTimeErrorDataToFile(0)

    do {
      numIterations += 1
      println(s"\t\t\tIteration $numIterations")
      totalDelta = iterativeUpdate(numIterations)

      if (printErrorForEachIteration) {
        println(s"\t\t\tTotal delta: $totalDelta, threshold: ${convergenceThreshold * N * clusters.size * totalDistribution.sum}")
        printExperimentTimeErrorDataToFile(numIterations)
        println(s"\t\t\tError: ${error(trueResult, getSolution)}")
      }

    } while (totalDelta >= convergenceThreshold * N * clusters.size)
      // There may be alternative ways to define the convergence criteria,
      // e.g. compare directly with previous distribution, compare to given marginal distributions, max of all deltas, etc.
    println(s"\t\t\tVanilla IPF number of iterations: $numIterations")
    getSolution
  }

  /**
   * One iterative scaling update.
   * @param iteration Number of iteration.
   * @return totalDelta, total absolute change of probabilities
   */
  /* private */ def iterativeUpdate(iteration: Int): Double = {
    var totalDelta: Double = 0.0

    clusters.foreach { case Cluster(marginalVariables: Int, expectedMarginalDistribution: Array[Double]) =>
      totalDelta += IPFUtils.updateTotalDistributionBasedOnMarginalDistribution(querySize, totalDistribution, marginalVariables, expectedMarginalDistribution)
      if (printErrorForEachUpdate) {
        println(s"\t\t\tUpdating ${BitUtils.IntToSet(marginalVariables).mkString(":")}")
        printExperimentTimeErrorDataToFile(iteration)
        println(s"\t\t\tError: ${error(trueResult, getSolution)}")
//      clusters.foreach(cluster => {
//        print(s"${error(cluster.distribution, IPFUtils.getMarginalDistributionFromTotalDistribution(querySize, totalDistribution, cluster.variables))}, ")
//        println(s"Expected ${cluster.distribution.mkString(",")}, got ${IPFUtils.getMarginalDistributionFromTotalDistribution(querySize, totalDistribution, cluster.variables).mkString(",")}")
//      })
//      println()
      }
    }

    totalDelta
  }

  private def printExperimentTimeErrorDataToFile(iteration: Int): Unit = {
    val currentTime = System.nanoTime()
    val totalTime = (currentTime - Profiler.startTimers("Vanilla IPF Total")) / 1000
    val solveTime = (currentTime - Profiler.startTimers("Vanilla IPF Solve")) / 1000
    val currentError = error(trueResult, getSolution)
//    println(s"\t\tTotal Time: $totalTime, solveTime: $solveTime, Error: $currentError")
    // CubeName, Query, QSize, IPFTotalTime(us), IPFSolveTime(us), IPFErr
    timeErrorFileOut.println(s"$cubeName, $query, $querySize, $iteration, $totalTime, $solveTime, $currentError")
  }
}
