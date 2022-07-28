package core.solver.iterativeProportionalFittingSolver

import core.solver.SolverTools.error
import util.{BitUtils, Profiler}

import java.io.PrintStream

/**
 * Vanilla version of linear proportional fitting.
 * TODO: Delete print statements for experiments/testing/demo.
 * @author Zhekai Jiang
 * @param querySize Total number of dimensions queried.
 */
class VanillaIPFSolver(override val querySize: Int,
                       val isExperimenting: Boolean = false,
                       val trueResult: Array[Double] = null, /* for experimentation only */
                       val timeErrorFileOut: PrintStream = null, /* for experimentation only */
                       val cubeName: String = "", val query: String = "" /* for experimentation only */)
  extends IPFSolver(querySize, "Vanilla IPF") {

  /**
   * Obtain the solution, un-normalized
   * TODO: confirm about convergence criteria (new idea – compare directly with previous distribution / compare to given marginal distributions?)
   * @return solution as a one-dimensional array (values encoded as bits of 1 in index).
   */
  def solve(): Array[Double] = {
    println(s"\t\t\tVanilla IPF number of entries (|C| 2^|Q|): ${clusters.size << querySize}")

    var totalDelta: Double = 0.0
    var numIterations = 0

    if (isExperimenting)
      printExperimentTimeErrorDataToFile()

    do {
      numIterations += 1
      println(s"\t\t\tIteration $numIterations")
      totalDelta = iterativeUpdate()

      if (isExperimenting) {
        printExperimentTimeErrorDataToFile()
        println(s"\t\t\tTotal delta: $totalDelta, threshold: ${convergenceThreshold * N * clusters.size * totalDistribution.sum}")
        println(s"\t\t\tError: ${error(trueResult, getSolution)}")
      }

    } while (totalDelta >= convergenceThreshold * N * clusters.size)
    println(s"\t\t\tVanilla IPF number of iterations: $numIterations")
    getSolution
  }

  /**
   * One iterative scaling update.
   * @return totalDelta, total absolute change of probabilities
   */
  /* private */ def iterativeUpdate(): Double = {
    var totalDelta: Double = 0.0

    clusters.foreach { case Cluster(marginalVariables: Int, expectedMarginalDistribution: Array[Double]) =>
      totalDelta += IPFUtils.updateTotalDistributionBasedOnMarginalDistribution(querySize, totalDistribution, marginalVariables, expectedMarginalDistribution)
      println(s"\t\t\tUpdating ${BitUtils.IntToSet(marginalVariables).mkString(":")}")
//      clusters.foreach(cluster => {
//        print(s"${error(cluster.distribution, IPFUtils.getMarginalDistributionFromTotalDistribution(querySize, totalDistribution, cluster.variables))}, ")
//        println(s"Expected ${cluster.distribution.mkString(",")}, got ${IPFUtils.getMarginalDistributionFromTotalDistribution(querySize, totalDistribution, cluster.variables).mkString(",")}")
//      })
//      println()
    }

    totalDelta
  }

  private def printExperimentTimeErrorDataToFile(): Unit = {
    val currentTime = System.nanoTime()
    val totalTime = (currentTime - Profiler.startTimers("Vanilla IPF Total")) / 1000
    val solveTime = (currentTime - Profiler.startTimers("Vanilla IPF Solve")) / 1000
    val currentError = error(trueResult, getSolution)
//    println(s"\t\tTotal Time: $totalTime, solveTime: $solveTime, Error: $currentError")
    // CubeName, Query, QSize, IPFTotalTime(us), IPFSolveTime(us), IPFErr
    timeErrorFileOut.println(s"$cubeName, $query, $querySize, $totalTime, $solveTime, $currentError")
  }
}
