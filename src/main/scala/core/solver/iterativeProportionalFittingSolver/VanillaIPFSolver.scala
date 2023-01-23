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
  var numIterations = 0
  /**
   * Obtain the solution, un-normalized
   * @return solution as a one-dimensional array (values encoded as bits of 1 in index).
   */
  def solve(): Array[Double] = {
    //println(s"\t\t\tVanilla IPF number of entries (|C| 2^|Q|): ${clusters.size << querySize}")
    var totalDelta: Double = 0.0
    numIterations = 0

    if (printErrorForEachIteration || printErrorForEachUpdate)
      printExperimentTimeErrorDataToFile(0)

    do {
      numIterations += 1
      //println(s"\t\t\tIteration $numIterations")
      totalDelta = iterativeUpdate(numIterations)

      if (printErrorForEachIteration) {
        println(s"\t\t\tTotal delta: $totalDelta, threshold: ${convergenceThreshold * N * clusters.size * totalDistribution.sum}")
        printExperimentTimeErrorDataToFile(numIterations)
        println(s"\t\t\tError: ${error(trueResult, getSolution)}")
      }

    } while (totalDelta >= convergenceThreshold * N * clusters.size)
    // There may be alternative ways to define the convergence criteria,
    // e.g. compare directly with previous distribution, compare to given marginal distributions, max of all deltas, etc.
    //println(s"\t\t\tVanilla IPF number of iterations: $numIterations")
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
      totalDelta += Profiler("UpdateMarginal Old") { IPFUtils.updateTotalDistributionBasedOnMarginalDistribution(querySize, totalDistribution, marginalVariables, expectedMarginalDistribution) }
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


class NewVanillaIPFSolver(override val querySize: Int,
                          override val printErrorForEachIteration: Boolean = false,
                          override val printErrorForEachUpdate: Boolean = false,
                          override val trueResult: Array[Double] = null, /* for experimentation only */
                          override val timeErrorFileOut: PrintStream = null, /* for experimentation only */
                          override val cubeName: String = "", override val query: String = "" /* for experimentation only */) extends
  VanillaIPFSolver(querySize, printErrorForEachIteration, printErrorForEachUpdate, trueResult, timeErrorFileOut, cubeName, query) {
  val temp = new Array[Double](1 << querySize)
  override def iterativeUpdate(iteration: Int): Double = {
    var totalDelta: Double = 0.0
    clusters.foreach { case Cluster(marginalVariables: Int, expectedMarginalDistribution: Array[Double]) =>
      totalDelta += Profiler("UpdateMarginal New") { IPFUtils.updateTotalDistributionBasedOnMarginalDistributionNew2(querySize, totalDistribution, marginalVariables, expectedMarginalDistribution, temp) }
    }

    totalDelta
  }
}

class MSTVanillaIPFSolver(querySize: Int) extends IPFSolver(querySize, "MST IPF") {
  var numIterations = 0
  val temp = new Array[Double](1 << querySize)
  var orderedClusters = List[Cluster]()

  def constructMST() = {
    val clustersArray = clusters.toVector
    clustersArray.take(5).foreach(println)
    //println("Clusters: " + clustersArray.indices.map(i => s"$i:${BitUtils.IntToSet(clustersArray(i).variables)}").mkString("\n"))
    val adjacencyMatrix = Array.fill(clustersArray.size)(Array.fill(clustersArray.size)(0))
    var i = 0
    var minIdx = 0
    var minDist = Int.MaxValue
    while (i < clustersArray.size) {
      val distZero = BitUtils.sizeOfSet(clustersArray(i).variables)
      if (distZero < minDist) {
        minIdx = i
        minDist = distZero
      }
      var j = i + 1
      while (j < clustersArray.size) {
        val xor = clustersArray(i).variables ^ clustersArray(j).variables
        val dist = BitUtils.sizeOfSet(xor)
        adjacencyMatrix(i)(j) = dist
        adjacencyMatrix(j)(i) = dist
        j += 1
      }
      i += 1
    }
    val available = Array.fill(clustersArray.size)(true)
    val queue = collection.mutable.PriorityQueue[(Int, Int, Int)]()(Ordering.by(-_._3)) //lowest weight
    available(minIdx) = false
    i = 0
    while (i < clustersArray.size) {
      if (i != minIdx) {
        queue += ((minIdx, i, adjacencyMatrix(minIdx)(i)))
      }
      i += 1
    }
    var mstAdjacencyList = Array.fill(clustersArray.size)(List[Int]())
    val parents = Array.fill(clustersArray.size)(-1)
    while (!queue.isEmpty) {
      val (i, j, w) = queue.dequeue()
      if (available(i)) { //add edge j -> i
        available(i) = false
        parents(i) = j
        mstAdjacencyList(j) = i :: mstAdjacencyList(j)
        var k = 0
        //println(s"Edge $j (${BitUtils.IntToSet(clustersArray(j).variables)}) -> $i (${BitUtils.IntToSet(clustersArray(i).variables)}) with weight $w")
        while (k < clustersArray.size) {
          if (available(k)) {
            queue += ((i, k, adjacencyMatrix(i)(k)))
          }
          k += 1
        }
      } else if (available(j)) { //add edge i -> j
        available(j) = false
        mstAdjacencyList(i) = j :: mstAdjacencyList(i)
        parents(j) = i
        var k = 0
        //println(s"Edge $i (${BitUtils.IntToSet(clustersArray(i).variables)}) -> $j (${BitUtils.IntToSet(clustersArray(j).variables)}) with weight $w")
        while (k < clustersArray.size) {
          if (available(k)) {
            queue += ((j, k, adjacencyMatrix(j)(k)))
          }
          k += 1
        }
      }
    }

    def pre(result: List[Int], idx: Int): List[Int] = {
      var newresult = idx :: result
      mstAdjacencyList(idx).foreach { j =>
        newresult = pre(newresult, j)
      }
      newresult
    }
    orderedClusters = pre(Nil, minIdx).reverse.map(clustersArray(_))
  }

  def transformToMarginalDistributionOf(currentMarginalVariables: Int, nextMarginalVariables: Int) = {
    val xor = currentMarginalVariables ^ nextMarginalVariables
    var h = 1
    while (h < N) {
      if ((h & xor) != 0) {
        if ((h & nextMarginalVariables) == 0) { //not in next, but in current, so aggregate the dim away
          var i = 0
          while (i < N) {
            var j = i
            while (j < i + h) {
              totalDistribution(j) += totalDistribution(j + h)
              j += 1
            }
            i += (h << 1)
          }
        } else { //undo aggregation
          var i = 0
          while (i < N) {
            var j = i
            while (j < i + h) {
              totalDistribution(j) -= totalDistribution(j + h)
              j += 1
            }
            i += (h << 1)
          }
        }
      }
      h <<= 1
    }
  }

  def iterativeUpdate(iteration: Int): Double = {
    var totalDelta = 0.0
    val N = totalDistribution.length
    var currentMarginalVariables = N - 1 //initially all variables are included

    orderedClusters.foreach { case Cluster(nextMarginalVariables, expectedMarginalDistribution) =>
      transformToMarginalDistributionOf(currentMarginalVariables, nextMarginalVariables)
      val nextNonMarginalVariables = (N - 1) ^ nextMarginalVariables
      //compare current marginal with expected marginal and update
      var i = 0
      var i0 = 0
      while (i < N) {
        if ((i & nextNonMarginalVariables) == 0) { //i is the first entry for this marginal variable // has all non-marginal dims set to 0
          temp(i) = if (totalDistribution(i).abs < 1e-9) 0 else
            expectedMarginalDistribution(i0) / totalDistribution(i)
          totalDelta += (expectedMarginalDistribution(i0) - totalDistribution(i)).abs
          totalDistribution(i) = expectedMarginalDistribution(i0)
          i0 += 1
        } else { //has some non-marginal dim set to 1
          val i1 = i & nextMarginalVariables // set all non-marginal dims to zero
          totalDistribution(i) *= temp(i1)
        }
        i += 1
      }
      currentMarginalVariables = nextMarginalVariables
    }
    transformToMarginalDistributionOf(currentMarginalVariables, N - 1)
    totalDelta
  }

  override def solve(): Array[Double] = {
    Profiler("MST.ConstructMST"){constructMST()}
    var totalDelta: Double = 0.0
    numIterations = 0

    do {
      numIterations += 1
      //println(s"\t\t\tIteration $numIterations")
      totalDelta = Profiler("UpdateMarginal MST"){iterativeUpdate(numIterations)}
    } while (totalDelta >= convergenceThreshold * N * clusters.size)
    getSolution
  }
}