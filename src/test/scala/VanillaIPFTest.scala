package core.solver.iterativeProportionalFittingSolver

import core.solver.SolverTools.error
import core.solver.moment.Strategy._
import core.solver.moment.MomentSolverAll
import org.junit.Test
import util.{Bits, Profiler}

import scala.util.Random

/**
 * TODO: extending FlatSpec with Matchers in org.scalatest._ ? (Cannot resolve symbol)
 * @author Zhekai Jiang
 */
class VanillaIPFTest {
  private val eps = 1e-3
  implicit def listToInt = Bits.toInt(_)
  @Test
  def testAdd(): Unit = {
    val solver = new VanillaIPFSolver(3)
    solver.add(List(0, 1), Array(0.1, 0.2, 0.3, 0.4))
    solver.add(List(1, 2), Array(0.2, 0.1, 0.3, 0.4))
    solver.add(List(0, 2), Array(0.3, 0.4, 0.1, 0.2))
    assert(solver.clusters.exists(cluster =>
      cluster.variables == 0x3
        && cluster.distribution(0x0) == 0.1
        && cluster.distribution(0x1) == 0.2
        && cluster.distribution(0x2) == 0.3
        && cluster.distribution(0x3) == 0.4
    ))
  }

  @Test
  def testGetAllVariablesValuesAsInt(): Unit = {
    val allVariablesValuesAsInt = IPFUtils.encodeAllVariablesValues(5, 18, 1 /* 01 */, 5 /* 101 */)
    assert(allVariablesValuesAsInt == 11)
  }

  @Test
  def testGetMarginalProbability2D(): Unit = {
    val totalDistribution = Array(0.1, 0.2, 0.3, 0.4)

    assertApprox(IPFUtils.getMarginalProbability(2, totalDistribution, 0, 0), 1.0)

    assertApprox(IPFUtils.getMarginalProbability(2, totalDistribution, 1, 0), 0.4)
    assertApprox(IPFUtils.getMarginalProbability(2, totalDistribution, 1, 1), 0.6)

    assertApprox(IPFUtils.getMarginalProbability(2, totalDistribution, 2, 0), 0.3)
    assertApprox(IPFUtils.getMarginalProbability(2, totalDistribution, 2, 1), 0.7)

    assertApprox(IPFUtils.getMarginalProbability(2, totalDistribution, 3, 0), 0.1)
    assertApprox(IPFUtils.getMarginalProbability(2, totalDistribution, 3, 1), 0.2)
    assertApprox(IPFUtils.getMarginalProbability(2, totalDistribution, 3, 2), 0.3)
    assertApprox(IPFUtils.getMarginalProbability(2, totalDistribution, 3, 3), 0.4)
  }

  @Test
  def testGetMarginalDistribution3D(): Unit = {
    val totalDistribution = (1 to 8).map(_ / 36.0).toArray

    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 1, 0), 16.0/36.0)
    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 1, 1), 20.0/36.0)

    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 2, 0), 14.0/36.0)
    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 2, 1), 22.0/36.0)

    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 4, 0), 10.0/36.0)
    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 4, 1), 26.0/36.0)

    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 3, 0), 6.0/36.0)
    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 3, 1), 8.0/36.0)
    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 3, 2), 10.0/36.0)
    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 3, 3), 12.0/36.0)

    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 5, 0), 4.0/36.0)
    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 5, 1), 6.0/36.0)
    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 5, 2), 12.0/36.0)
    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 5, 3), 14.0/36.0)

    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 6, 0), 3.0/36.0)
    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 6, 1), 7.0/36.0)
    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 6, 2), 11.0/36.0)
    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 6, 3), 15.0/36.0)

    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 7, 0), 1.0/36.0)
    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 7, 1), 2.0/36.0)
    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 7, 2), 3.0/36.0)
    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 7, 3), 4.0/36.0)
    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 7, 4), 5.0/36.0)
    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 7, 5), 6.0/36.0)
    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 7, 6), 7.0/36.0)
    assertApprox(IPFUtils.getMarginalProbability(3, totalDistribution, 7, 7), 8.0/36.0)
  }

  @Test
  def testUpdateDistributionForOneValueInstance2D(): Unit = {
    val solver = new VanillaIPFSolver(2) // Initialized to uniform — all 0.25's
    IPFUtils.updateDistributionForOneMarginalValue(2, solver.totalDistribution, 1, 0, 4.0/5.0)
    for ((i, p) <- Seq((0, 0.2), (1, 0.25), (2, 0.2), (3, 0.25))) {
      assertApprox(solver.totalDistribution(i), p)
    }
    IPFUtils.updateDistributionForOneMarginalValue(2, solver.totalDistribution, 1, 1, 6.0/5.0)
    for ((i, p) <- Seq((0, 0.2), (1, 0.3), (2, 0.2), (3, 0.3))) {
      assertApprox(solver.totalDistribution(i), p)
    }
    IPFUtils.updateDistributionForOneMarginalValue(2, solver.totalDistribution, 2, 0, 3.0/5.0)
    for ((i, p) <- Seq((0, 0.12), (1, 0.18), (2, 0.2), (3, 0.3))) {
      assertApprox(solver.totalDistribution(i), p)
    }
    IPFUtils.updateDistributionForOneMarginalValue(2, solver.totalDistribution, 2, 1, 7.0/5.0)
    for ((i, p) <- Seq((0, 0.12), (1, 0.18), (2, 0.28), (3, 0.42))) {
      assertApprox(solver.totalDistribution(i), p)
    }
  }

  @Test
  def testUpdateDistributionForOneValueInstance3D(): Unit = {
    val solver = new VanillaIPFSolver(3)
    solver.totalDistribution = (1 to 8).map(_ / 36.0).toArray
    IPFUtils.updateDistributionForOneMarginalValue(3, solver.totalDistribution, 2, 0, 18.0/14.0)
    for ((i, p) <- Seq((0, 1.0/28.0), (1, 2.0/28.0), (2, 3.0/36.0), (3, 4.0/36.0), (4, 5.0/28.0), (5, 6.0/28.0), (6, 7.0/36.0), (7, 8.0/36.0))) {
      assertApprox(solver.totalDistribution(i), p)
    }
    IPFUtils.updateDistributionForOneMarginalValue(3, solver.totalDistribution, 2, 1, 18.0/22.0)
    for ((i, p) <- Seq((0, 1.0/28.0), (1, 2.0/28.0), (2, 3.0/44.0), (3, 4.0/44.0), (4, 5.0/28.0), (5, 6.0/28.0), (6, 7.0/44.0), (7, 8.0/44.0))) {
      assertApprox(solver.totalDistribution(i), p)
    }
  }

  @Test
  def testUpdateDistribution2D(): Unit = {
    val solver = new VanillaIPFSolver(2)
    solver.add(Seq(0), Array(0.4, 0.6))
    solver.add(Seq(1), Array(0.3, 0.7))
    val totalDelta = solver.iterativeUpdate()
    for ((i, p) <- Seq((0, 0.12), (1, 0.18), (2, 0.28), (3, 0.42))) {
      assertApprox(solver.totalDistribution(i), p)
    }
    assertApprox(totalDelta, 0.6)
  }

  @Test
  def testSolve2D(): Unit = {
    val solver = new VanillaIPFSolver(2)
    solver.add(Seq(0), Array(0.4, 0.6))
    solver.add(Seq(1), Array(0.3, 0.7))
    solver.solve()
    checkSolutionConsistency(solver)
  }

  @Test
  def testSolve3D(): Unit = {
    val solver = new VanillaIPFSolver(3)
    solver.add(Seq(0), Array(0.25, 0.75))
    solver.add(Seq(0, 2), Array(0.1, 0.5, 0.15, 0.25))
    solver.add(Seq(1, 2), Array(0.2, 0.4, 0.3, 0.1))
    solver.solve()
    checkSolutionConsistency(solver)
  }

  @Test
  def testSolve3DWith0(): Unit = {
    val solver = new VanillaIPFSolver(3)
    solver.add(Seq(0), Array(0.25, 0.75))
    solver.add(Seq(0, 2), Array(0.1, 0.5, 0.15, 0.25))
    solver.add(Seq(1, 2), Array(0.3, 0.3, 0.4, 0.0))
    solver.solve()
    checkSolutionConsistency(solver)
    println(solver.solution.mkString(" "))
  }

  @Test
  def testSolve_bigCase_compareWithMomentSolver(): Unit = {
    val numDimensions = 10
    val numClusters = 100

    val vanillaIPFSolver = new VanillaIPFSolver(numDimensions)
    val momentSolver = new MomentSolverAll[Double](numDimensions, CoMoment4)

    val randomGenerator = new Random()

    var distribution: IndexedSeq[Double] = for (_ <- 0 until 1 << numDimensions) yield randomGenerator.nextInt(100).toDouble + 1
    val sum = distribution.sum
    distribution = distribution.map(_ / sum)

    for (_ <- 0 until numClusters) {
      val marginalVariables = randomGenerator.nextInt(1 << numDimensions)
      val numMarginalVariables = IPFUtils.getNumOnesInBinary(marginalVariables)
      val numNonMarginalVariables = numDimensions - numMarginalVariables

      val marginalDistribution: Array[Double] = Array.fill(1 << numMarginalVariables)(0)
      for (marginalVariablesValues <- 0 until 1 << numMarginalVariables) {
        for (nonMarginalVariablesValues <- 0 until 1 << numNonMarginalVariables) {
          val nonMarginalVariables = ((1 << numDimensions) - 1) ^ marginalVariables
          val allVariablesValues = Bits.unproject(marginalVariablesValues, marginalVariables) | Bits.unproject(nonMarginalVariablesValues, nonMarginalVariables)
          marginalDistribution(marginalVariablesValues) += distribution(allVariablesValues)
        }
      }

      vanillaIPFSolver.add(marginalVariables, marginalDistribution)
      momentSolver.add(marginalVariables, marginalDistribution)
    }

    momentSolver.fillMissing()

    Profiler("Vanilla IPF") {
      vanillaIPFSolver.solve()
    }
    println("Vanilla IPF Error = " + error(distribution.toArray, vanillaIPFSolver.totalDistribution))

    Profiler("Moment") {
      momentSolver.fastSolve()
    }
    println("Moment Error = " + error(distribution.toArray, momentSolver.solution))

    println("Difference (using error measure) = " + error(momentSolver.solution, vanillaIPFSolver.totalDistribution))

    println("Max difference out of total sum = " +
      (0 until 1 << numDimensions).map(i => (momentSolver.solution(i) - vanillaIPFSolver.totalDistribution(i)).abs).max
    )

    println("Moment Entropy = " + momentSolver.solution.map(p => if (p != 0) -p * math.log(p) else 0).sum )
    println("Vanilla IPF Entropy = " + vanillaIPFSolver.totalDistribution.map(p => if (p != 0) - p * math.log(p) else 0).sum )

    Profiler.print()
  }

  @Test
  def testBreakingIPF(): Unit = {
    val numDimensions = 15

    val vanillaIPFSolver = new VanillaIPFSolver(numDimensions)

    val randomGenerator = new Random()

    var distribution: IndexedSeq[Double] = for (_ <- 0 until 1 << numDimensions) yield randomGenerator.nextInt(100).toDouble + 1
    val sum = distribution.sum
    distribution = distribution.map(_ / sum)

    for (marginalVariables <- Seq((1 << 10) - 1, ((1 << 10) - 1) << 5)) {
      val numMarginalVariables = IPFUtils.getNumOnesInBinary(marginalVariables)
      val numNonMarginalVariables = numDimensions - numMarginalVariables

      val marginalDistribution: Array[Double] = Array.fill(1 << numMarginalVariables)(0)
      for (marginalVariablesValues <- 0 until 1 << numMarginalVariables) {
        for (nonMarginalVariablesValues <- 0 until 1 << numNonMarginalVariables) {
          val nonMarginalVariables = ((1 << numDimensions) - 1) ^ marginalVariables
          val allVariablesValues = Bits.unproject(marginalVariablesValues, marginalVariables) | Bits.unproject(nonMarginalVariablesValues, nonMarginalVariables)
          marginalDistribution(marginalVariablesValues) += distribution(allVariablesValues)
        }
      }

      vanillaIPFSolver.add(Bits.fromInt(marginalVariables).reverse, marginalDistribution)
    }

    vanillaIPFSolver.solve()
  }

  private def assertApprox: (Double, Double) => Unit = (a, b) => assert((a - b).abs < eps)

  private def checkSolutionConsistency(solver: VanillaIPFSolver): Unit = {
    for (Cluster(marginalVariablesAsInt, expectedMarginalDistribution) <- solver.clusters) {
      val marginalVariables = Bits.fromInt(marginalVariablesAsInt).reverse
      for (marginalVariablesValues <- 0 until (1 << marginalVariables.length)) {
        assertApprox(
          IPFUtils.getMarginalProbability(solver.querySize, solver.totalDistribution, marginalVariablesAsInt, marginalVariablesValues),
          expectedMarginalDistribution(marginalVariablesValues)
        )
      }
    }
  }
}