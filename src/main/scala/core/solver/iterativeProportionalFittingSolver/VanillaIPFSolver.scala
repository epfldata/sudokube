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
                       val cubeName: String = "", val query: String = "" /* for experimentation only */) extends IPFSolver(querySize) {

  /**
   * Add a new known marginal distribution as a cluster.
   * @param marginalVariables Sequence of marginal variables.
   * @param marginalDistribution Marginal distribution as a one-dimensional array (values encoded as bits of 1 in index).
   */
  override def add(marginalVariables: Int, marginalDistribution: Array[Double]): Unit = {
    normalizationFactor = marginalDistribution.sum
    clusters = Cluster(marginalVariables, marginalDistribution.map(_ / normalizationFactor)) :: clusters
  }

  /**
   * Obtain the solution, un-normalized
   * TODO: confirm about convergence criteria (new idea – compare directly with previous distribution / compare to given marginal distributions?)
   * @return solution as a one-dimensional array (values encoded as bits of 1 in index).
   */
  def solve(): Array[Double] = {
    println(s"\t\t\tVanilla IPF number of updates per iteration (|C| 2^|Q|): ${clusters.size << querySize}")

    var totalDelta: Double = 0.0
    var numIterations = 0

//    if (isExperimenting)
//      printExperimentTimeErrorDataToFile()

//    print(", ")
//    clusters.foreach(cluster => print(s"${Bits.fromInt(cluster.variables).mkString(":")}, "))
//    println()
    do {
      numIterations += 1
      println(s"\t\t\tIteration $numIterations")
      totalDelta = iterativeUpdate()
//      verifySolution()

//      if (isExperimenting) {
//        printExperimentTimeErrorDataToFile()
              println(s"\t\t\tTotal delta: $totalDelta, threshold: ${convergenceThreshold * N * clusters.length * totalDistribution.sum}")
              println(s"\t\t\tError: ${error(trueResult, getSolution)}")
//      }

    } while (totalDelta >= convergenceThreshold * N * clusters.length)
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
