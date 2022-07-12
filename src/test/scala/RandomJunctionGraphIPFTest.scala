import core.solver.iterativeProportionalFittingSolver.{IPFUtils, RandomJunctionGraphIPFSolver}
import org.junit.Test
import util.Bits

import scala.util.Random

class RandomJunctionGraphIPFTest {
  val eps: Double = 1e-4

  @Test
  def test(): Unit = {
    val randomGenerator = new Random()
    val data: Array[Double] = Array.fill(1 << 6)(0)
    (0 until 1 << 6).foreach(i => data(i) = randomGenerator.nextInt(100))

    val solver = new RandomJunctionGraphIPFSolver(6)
    val marginalDistributions: Map[Seq[Int], Array[Double]] =
      Seq(Seq(0,1), Seq(1,2), Seq(2,3), Seq(0,3,4), Seq(4,5)).map(marginalVariables =>
        marginalVariables -> IPFUtils.getMarginalDistribution(6, data, marginalVariables.size, Bits.toInt(marginalVariables))
      ).toMap

    marginalDistributions.foreach { case (marginalVariables, clustersDistribution) =>
      solver.add(marginalVariables, clustersDistribution)
    }

    solver.solve()

    marginalDistributions.foreach { case (marginalVariables, marginalDistribution) =>
      marginalDistribution.indices.foreach(marginalVariablesValues => {
        println(s"Expected ${marginalDistribution(marginalVariablesValues)}, Got ${IPFUtils.getMarginalProbability(6, solver.totalDistribution, Bits.toInt(marginalVariables), marginalVariablesValues)* solver.normalizationFactor}")
        assertApprox(
          marginalDistribution(marginalVariablesValues),
          IPFUtils.getMarginalProbability(6, solver.totalDistribution, Bits.toInt(marginalVariables), marginalVariablesValues)* solver.normalizationFactor
        )
      })
    }
  }

  private def assertApprox: (Double, Double) => Unit = (a, b) => assert(((a - b) / a).abs < eps)
}
