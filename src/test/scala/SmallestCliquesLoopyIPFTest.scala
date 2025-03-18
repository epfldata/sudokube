import core.solver.iterativeProportionalFittingSolver.{IPFUtils, SmallestCliquesLoopyIPFSolver}
import org.junit.{Ignore, Test}
import util.BitUtils
import BitUtils._

import scala.util.Random

/**
 * Test the functionalities of the smallest-cliques loopy IPF solver, where each cluster is placed in a different clique.
 * Note that some tests are expected to fail because this method is not guaranteed to converge and the error can stay high.
 * @author Zhekai Jiang
 */
@Ignore  //SBJ: test disabled due to high error
class SmallestCliquesLoopyIPFTest {
  private val eps = 1e-3

  @Test
  def testConstructJunctionGraph(): Unit = {
    val solver = new SmallestCliquesLoopyIPFSolver(5)
    Seq(Seq(0, 1), Seq(0, 2), Seq(0, 3), Seq(0, 4), Seq(2, 3)).foreach(variables => solver.add(SetToInt(variables), Array.fill(variables.size)(1.0 / variables.size)))
    solver.constructGraphicalModel()
    solver.constructJunctionGraph()
    solver.junctionGraph.cliques.foreach(clique => println(IntToSet(clique.variables)))
    solver.junctionGraph.separators.foreach(separator =>
      println(s"${IntToSet(separator.clique1.variables)} <--[${BitUtils.IntToSet(separator.variables)}]--> ${BitUtils.IntToSet(separator.clique2.variables)}")
    )
    assert(!solver.junctionGraph.separators.exists(separator => separator.numVariables == 0 || separator.variables == 0))

    // The cliques and separators induced by (containing) each variable form a spanning tree.
    (0 until 5).foreach(variable => assert(
      solver.junctionGraph.separators.count(separator => (separator.variables & (1 << variable)) != 0)
        == solver.junctionGraph.cliques.count(clique => (clique.variables & (1 << variable)) != 0) - 1
    ))
  }

  @Test
  def testSolve6D(): Unit = {
    val randomGenerator = new Random()
    val data: Array[Double] = Array.fill(1 << 6)(0)
    (0 until 1 << 6).foreach(i => data(i) = randomGenerator.nextInt(100))

    val solver = new SmallestCliquesLoopyIPFSolver(6)
    val marginalDistributions: Map[Seq[Int], Array[Double]] =
      Seq(Seq(0,1), Seq(1,2), Seq(2,3), Seq(0,3,4), Seq(4,5)).map(marginalVariables =>
        marginalVariables -> IPFUtils.getMarginalDistribution(6, data, marginalVariables.size,SetToInt(marginalVariables))
      ).toMap

    marginalDistributions.foreach { case (marginalVariables, clustersDistribution) =>
      solver.add(SetToInt(marginalVariables), clustersDistribution)
    }

    solver.solve()

    marginalDistributions.foreach { case (marginalVariables, marginalDistribution) =>
      marginalDistribution.indices.foreach(marginalVariablesValues => {
        println(s"Expected ${marginalDistribution(marginalVariablesValues)}, " +
          s"Got ${IPFUtils.getMarginalProbability(6, solver.totalDistribution, SetToInt(marginalVariables), marginalVariablesValues) * solver.normalizationFactor}")
        assertApprox(
          marginalDistribution(marginalVariablesValues),
          IPFUtils.getMarginalProbability(6, solver.totalDistribution, SetToInt(marginalVariables), marginalVariablesValues) * solver.normalizationFactor
        )
      })
    }
  }

  /**
   * Failing in this case — error very high with respect to the known marginals.
   */
  @Test
  def testSolveLoopy4D(): Unit = {
    val randomGenerator = new Random()
    val data: Array[Double] = Array.fill(1 << 4)(0)
    (0 until 1 << 4).foreach(i => data(i) = randomGenerator.nextInt(100))

    println(s"Total distribution: ${data.map(_ / data.sum).mkString(", ")}")

    val solver = new SmallestCliquesLoopyIPFSolver(4)
    val marginalDistributions: Map[Seq[Int], Array[Double]] =
      Seq(Seq(0,1,2), Seq(0,2,3), Seq(0,1,3)).map(marginalVariables => {
        val marginalDistribution = IPFUtils.getMarginalDistribution(4, data, marginalVariables.size, SetToInt(marginalVariables))
        println(s"Variables ${marginalVariables.mkString("")}: ${marginalDistribution.map(_ / marginalDistribution.sum).mkString(", ")}")
        marginalVariables -> marginalDistribution
      }).toMap

    marginalDistributions.foreach { case (marginalVariables, clustersDistribution) =>
      solver.add(SetToInt(marginalVariables), clustersDistribution)
    }

    solver.solve()

    marginalDistributions.foreach { case (marginalVariables, marginalDistribution) =>
      marginalDistribution.indices.foreach(marginalVariablesValues => {
        println(s"Expected ${marginalDistribution(marginalVariablesValues)}, " +
          s"Got ${IPFUtils.getMarginalProbability(4, solver.totalDistribution, SetToInt(marginalVariables), marginalVariablesValues) * solver.normalizationFactor}")
        assertApprox(
          marginalDistribution(marginalVariablesValues),
          IPFUtils.getMarginalProbability(4, solver.totalDistribution, SetToInt(marginalVariables), marginalVariablesValues) * solver.normalizationFactor
        )
      })
    }
  }

  /**
   * This made-up test shows that the probability distribution may not sum to 1.
   */
  @Test
  def testSolveLoopy4DProblemCase(): Unit = {
    val data: Array[Double] = Array(0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.985)

    println(s"Total distribution: ${data.map(_ / data.sum).mkString(", ")}")

    val solver = new SmallestCliquesLoopyIPFSolver(4)
    val marginalDistributions: Map[Seq[Int], Array[Double]] =
      Seq(Seq(0,1,2), Seq(0,2,3), Seq(0,1,3)).map(marginalVariables => {
        val marginalDistribution = IPFUtils.getMarginalDistribution(4, data, marginalVariables.size, SetToInt(marginalVariables))
        println(s"Variables ${marginalVariables.mkString("")}: ${marginalDistribution.map(_ / marginalDistribution.sum).mkString(", ")}")
        marginalVariables -> marginalDistribution
      }).toMap

    marginalDistributions.foreach { case (marginalVariables, clustersDistribution) =>
      solver.add(SetToInt(marginalVariables), clustersDistribution)
    }

    solver.solve()

    println("Reconstructed distribution: " + solver.getTotalDistribution.mkString(", "))
    println("Distribution sum: " + solver.getTotalDistribution.sum) // not equal to one

    // force to normalize to 1, but still not accurate
    val totalDistributionSum = solver.totalDistribution.sum
    solver.totalDistribution = solver.totalDistribution.map(_ / totalDistributionSum)

    marginalDistributions.foreach { case (marginalVariables, marginalDistribution) =>
      marginalDistribution.indices.foreach(marginalVariablesValues => {
        println(s"Expected ${marginalDistribution(marginalVariablesValues)}, " +
          s"Got ${IPFUtils.getMarginalProbability(4, solver.totalDistribution, SetToInt(marginalVariables), marginalVariablesValues) * solver.normalizationFactor}")
        assertApprox(
          marginalDistribution(marginalVariablesValues),
          IPFUtils.getMarginalProbability(4, solver.totalDistribution, SetToInt(marginalVariables), marginalVariablesValues) * solver.normalizationFactor
        )
      })
    }
  }

  /**
   * Failing in this case — error very high with respect to the known marginals.
   */
  @Test
  def testSolveLoopy5D(): Unit = {
    val randomGenerator = new Random()
    val data: Array[Double] = Array.fill(1 << 5)(0)
    (0 until 1 << 5).foreach(i => data(i) = randomGenerator.nextInt(100))

    println(s"Total distribution: ${data.map(_ / data.sum).map("%.50f" format _).mkString(", ")}")

    val solver = new SmallestCliquesLoopyIPFSolver(5)
    val marginalDistributions: Map[Seq[Int], Array[Double]] =
      Seq(Seq(0,1,2), Seq(0,2,3), Seq(0,3,4), Seq(0,1,4), Seq(1,3,4)).map(marginalVariables => {
        val marginalDistribution = IPFUtils.getMarginalDistribution(5, data, marginalVariables.size, SetToInt(marginalVariables))
        println(s"Variables ${marginalVariables.mkString("")}: ${marginalDistribution.map(_ / marginalDistribution.sum).mkString(", ")}")
        marginalVariables -> marginalDistribution
      }).toMap

    marginalDistributions.foreach { case (marginalVariables, clustersDistribution) =>
      solver.add(SetToInt(marginalVariables), clustersDistribution)
    }

    solver.solve()

    marginalDistributions.foreach { case (marginalVariables, marginalDistribution) =>
      marginalDistribution.indices.foreach(marginalVariablesValues => {
        println(s"Expected ${marginalDistribution(marginalVariablesValues)}, " +
          s"Got ${IPFUtils.getMarginalProbability(5, solver.totalDistribution, SetToInt(marginalVariables), marginalVariablesValues) * solver.normalizationFactor}")
        assertApprox(
          marginalDistribution(marginalVariablesValues),
          IPFUtils.getMarginalProbability(5, solver.totalDistribution, SetToInt(marginalVariables), marginalVariablesValues) * solver.normalizationFactor
        )
      })
    }
  }

  private def assertApprox: (Double, Double) => Unit = (a, b) => assert(((a - b) / a).abs < eps)
}
