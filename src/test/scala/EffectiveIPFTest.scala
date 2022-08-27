import core.solver.iterativeProportionalFittingSolver.{Cluster, EffectiveIPFSolver, IPFUtils, JunctionGraph}
import org.junit.Test
import util.BitUtils

import scala.collection.mutable
import scala.util.Random

/**
 * Test the functionalities of the effective IPF solver.
 * @author Zhekai Jiang
 */
class EffectiveIPFTest {
  private val eps = 1e-3

  @Test
  def testConstructGraphicalModel(): Unit = {
    val solver = new EffectiveIPFSolver(6)
    solver.add(BitUtils.SetToInt(Seq(0, 1)), Array(0.5, 0.5))
    solver.add(BitUtils.SetToInt(Seq(0, 4)), Array(0.5, 0.5))
    solver.add(BitUtils.SetToInt(Seq(1, 2)), Array(0.5, 0.5))
    solver.add(BitUtils.SetToInt(Seq(2, 3, 4)), Array(1.0/3.0, 1.0/3.0, 1.0/3.0))
    solver.add(BitUtils.SetToInt(Seq(2, 3)), Array(0.5, 0.5))
    solver.add(BitUtils.SetToInt(Seq(3, 5)), Array(0.5, 0.5))
    solver.constructGraphicalModel()
    assert(solver.graphicalModel.nodes(0).adjacencyList.exists { case (destination, edge) => destination.variable == 1 && edge.clusters.size == 1 })
    assert(solver.graphicalModel.nodes(0).adjacencyList.exists { case (destination, edge) => destination.variable == 4 && edge.clusters.size == 1 })
    assert(solver.graphicalModel.nodes(1).adjacencyList.exists { case (destination, edge) => destination.variable == 0 && edge.clusters.size == 1 })
    assert(solver.graphicalModel.nodes(1).adjacencyList.exists { case (destination, edge) => destination.variable == 2 && edge.clusters.size == 1 })
    assert(solver.graphicalModel.nodes(2).adjacencyList.exists { case (destination, edge) => destination.variable == 1 && edge.clusters.size == 1 })
    assert(solver.graphicalModel.nodes(2).adjacencyList.exists { case (destination, edge) => destination.variable == 3 && edge.clusters.size == 2 })
    assert(solver.graphicalModel.nodes(2).adjacencyList.exists { case (destination, edge) => destination.variable == 4 && edge.clusters.size == 1 })
    assert(solver.graphicalModel.nodes(3).adjacencyList.exists { case (destination, edge) => destination.variable == 2 && edge.clusters.size == 2 })
    assert(solver.graphicalModel.nodes(3).adjacencyList.exists { case (destination, edge) => destination.variable == 4 && edge.clusters.size == 1 })
    assert(solver.graphicalModel.nodes(3).adjacencyList.exists { case (destination, edge) => destination.variable == 5 && edge.clusters.size == 1 })
    assert(solver.graphicalModel.nodes(5).adjacencyList.exists { case (destination, edge) => destination.variable == 3 && edge.clusters.size == 1 })
  }

  @Test
  def testConstructCliques(): Unit = {
    val solver = new EffectiveIPFSolver(6)
    solver.add(BitUtils.SetToInt(Seq(0, 1)), Array(0.5, 0.5))
    solver.add(BitUtils.SetToInt(Seq(0, 4)), Array(0.5, 0.5))
    solver.add(BitUtils.SetToInt(Seq(1, 2)), Array(0.5, 0.5))
    solver.add(BitUtils.SetToInt(Seq(2, 3, 4)), Array(1.0/3.0, 1.0/3.0, 1.0/3.0))
    solver.add(BitUtils.SetToInt(Seq(2, 3)), Array(0.5, 0.5))
    solver.add(BitUtils.SetToInt(Seq(3, 5)), Array(0.5, 0.5))
    solver.constructGraphicalModel()
    solver.junctionGraph.constructCliquesFromGraph(solver.graphicalModel)
    println(solver.junctionGraph.cliques.map(clique => BitUtils.IntToSet(clique.variables)))
    solver.clusters.foreach(cluster => assert(solver.junctionGraph.cliques.count(clique => clique.clusters.contains(cluster)) == 1))
  }

  @Test
  def testRetainMaximalCliques(): Unit = {
    val solver = new EffectiveIPFSolver(6)
    solver.add(BitUtils.SetToInt(Seq(0, 1)), Array(0.5, 0.5))
    solver.add(BitUtils.SetToInt(Seq(0, 4)), Array(0.5, 0.5))
    solver.add(BitUtils.SetToInt(Seq(1, 2)), Array(0.5, 0.5))
    solver.add(BitUtils.SetToInt(Seq(2, 3, 4)), Array(1.0/3.0, 1.0/3.0, 1.0/3.0))
    solver.add(BitUtils.SetToInt(Seq(2, 3)), Array(0.5, 0.5))
    solver.add(BitUtils.SetToInt(Seq(3, 5)), Array(0.5, 0.5))
    solver.constructGraphicalModel()
    solver.junctionGraph.constructCliquesFromGraph(solver.graphicalModel)
    solver.junctionGraph.deleteNonMaximalCliques()
    for (clique1 <- solver.junctionGraph.cliques; clique2 <- solver.junctionGraph.cliques) {
      if (clique1 != clique2) {
        assert((clique1.variables & clique2.variables) != clique1.variables)
      }
    }
  }

  @Test
  def testConstructCompleteCliqueGraph(): Unit = {
    val solver = new EffectiveIPFSolver(6)
    solver.add(BitUtils.SetToInt(Seq(0, 1)), Array(0.5, 0.5))
    solver.add(BitUtils.SetToInt(Seq(0, 4)), Array(0.5, 0.5))
    solver.add(BitUtils.SetToInt(Seq(1, 2)), Array(0.5, 0.5))
    solver.add(BitUtils.SetToInt(Seq(2, 3, 4)), Array(1.0/3.0, 1.0/3.0, 1.0/3.0))
    solver.add(BitUtils.SetToInt(Seq(2, 3)), Array(0.5, 0.5))
    solver.add(BitUtils.SetToInt(Seq(3, 5)), Array(0.5, 0.5))
    solver.constructGraphicalModel()
    solver.junctionGraph.constructCliquesFromGraph(solver.graphicalModel)
    solver.junctionGraph.deleteNonMaximalCliques()
    solver.junctionGraph.connectAllCliquesCompletely()
    for (clique1 <- solver.junctionGraph.cliques; clique2 <- solver.junctionGraph.cliques) {
      if (clique1 != clique2) {
        assert(clique1.adjacencyList.exists { case (destination, separator) =>
            destination == clique2 && separator.variables == (clique1.variables & clique2.variables)
        })
      }
    }
  }

  @Test
  def testConstructJunctionTree6D(): Unit = {
    val solver = new EffectiveIPFSolver(6)
    solver.add(BitUtils.SetToInt(Seq(0, 1)), Array(0.5, 0.5))
    solver.add(BitUtils.SetToInt(Seq(0, 4)), Array(0.5, 0.5))
    solver.add(BitUtils.SetToInt(Seq(1, 2)), Array(0.5, 0.5))
    solver.add(BitUtils.SetToInt(Seq(2, 3, 4)), Array(1.0/3.0, 1.0/3.0, 1.0/3.0))
    solver.add(BitUtils.SetToInt(Seq(2, 3)), Array(0.5, 0.5))
    solver.add(BitUtils.SetToInt(Seq(3, 5)), Array(0.5, 0.5))
    solver.constructGraphicalModel()
    solver.constructJunctionTree()
    assert(solver.junctionGraph.separators.size == solver.junctionGraph.cliques.size - 1)
  }

  @Test
  def testConstructJunctionTree9D(): Unit = {
    val solver = new EffectiveIPFSolver(9)
    val randomGenerator = new Random()
    for (_ <- 0 until 5) {
      val variables: Seq[Int] = BitUtils.IntToSet(randomGenerator.nextInt(1 << 9)).reverse
      println(variables)
      solver.add(BitUtils.SetToInt(variables), Array.fill(variables.size)(1.0 / variables.size))
    }
    solver.constructGraphicalModel()
    solver.constructJunctionTree()
    assert(solver.junctionGraph.separators.size == solver.junctionGraph.cliques.size - 1)
  }

  @Test
  def testConstructJunctionTree12D(): Unit = {
    val solver = new EffectiveIPFSolver(13)
    solver.add(BitUtils.SetToInt(Seq(0, 1)), Array.fill(4)(0.25))
    solver.add(BitUtils.SetToInt(Seq(1, 2)), Array.fill(4)(0.25))
    solver.add(BitUtils.SetToInt(Seq(2, 3)), Array.fill(4)(0.25))
    solver.add(BitUtils.SetToInt(Seq(0, 3, 4)), Array.fill(8)(0.125))
    solver.add(BitUtils.SetToInt(Seq(4, 5)), Array.fill(4)(0.25))
    solver.add(BitUtils.SetToInt(Seq(6, 7)), Array.fill(4)(0.25))
    solver.add(BitUtils.SetToInt(Seq(7, 8)), Array.fill(4)(0.25))
    solver.add(BitUtils.SetToInt(Seq(8, 9)), Array.fill(4)(0.25))
    solver.add(BitUtils.SetToInt(Seq(6, 9, 10)), Array.fill(8)(0.125))
    solver.add(BitUtils.SetToInt(Seq(10, 11)), Array.fill(4)(0.25))
    solver.add(BitUtils.SetToInt(Seq(12)), Array(0.5, 0.5))
    solver.constructGraphicalModel()
    solver.constructJunctionTree()
    assert(solver.junctionGraph.separators.size == solver.junctionGraph.cliques.size - 1)
  }

  @Test
  def testGetVariableIndicesWithinClique(): Unit = {
    assert(IPFUtils.getVariableIndicesWithinVariableSubset(20, 21) == 6)
  }

  @Test
  def testIterativeUpdateClique2D(): Unit = {
    val solver = new EffectiveIPFSolver(2)
    val clusters = mutable.Set[Cluster]()
    clusters += Cluster(1, Array(0.4, 0.6))
    clusters += Cluster(2, Array(0.3, 0.7))
    val clique = new JunctionGraph.Clique(3, clusters)
    val totalDelta = solver.updateCliqueBasedOnClusters(clique)
    for ((i, p) <- Seq((0, 0.12), (1, 0.18), (2, 0.28), (3, 0.42))) {
      assertApprox(clique.distribution(i), p)
    }
    assertApprox(totalDelta, 0.6)
  }

  @Test
  def testOneIterativeUpdate2Cliques(): Unit = {
    val solver = new EffectiveIPFSolver(4)
    val clique012 = new JunctionGraph.Clique(7, mutable.Set[Cluster](Cluster(3, Array(0.1, 0.2, 0.3, 0.4))))
    val clique023 = new JunctionGraph.Clique(13, mutable.Set[Cluster](Cluster(9, Array(0.3, 0.4, 0.1, 0.2))))
    val separator02 = new JunctionGraph.Separator(clique012, clique023)
    solver.junctionGraph.cliques += clique012
    solver.junctionGraph.cliques += clique023
    solver.junctionGraph.separators += separator02
    clique012.adjacencyList += clique023 -> separator02
    clique023.adjacencyList += clique012 -> separator02
    solver.dfsUpdate(clique012, solver.junctionGraph.cliques.clone(), mutable.Set[JunctionGraph.Separator]())
    val expectedResult012 = Array(0.05, 0.1, 0.15, 0.2, 0.05, 0.1, 0.15, 0.2)
    val expectedResult02 = Array(0.2, 0.3, 0.2, 0.3)
    val expectedResult023 = Array(0.15, 0.2, 0.15, 0.2, 0.05, 0.1, 0.05, 0.1)
    (0 until 8).foreach(i => assertApprox(clique012.distribution(i), expectedResult012(i)))
    (0 until 4).foreach(i => assertApprox(separator02.distribution(i), expectedResult02(i)))
    (0 until 8).foreach(i => assertApprox(clique023.distribution(i), expectedResult023(i)))
  }

  @Test
  def testGetTotalDistribution(): Unit = {
    val solver = new EffectiveIPFSolver(4)
    val clique012 = new JunctionGraph.Clique(7, mutable.Set[Cluster](Cluster(3, Array(0.1, 0.2, 0.3, 0.4))))
    val clique023 = new JunctionGraph.Clique(13, mutable.Set[Cluster](Cluster(9, Array(0.3, 0.4, 0.1, 0.2))))
    val separator02 = new JunctionGraph.Separator(clique012, clique023)
    solver.junctionGraph.cliques += clique012
    solver.junctionGraph.cliques += clique023
    solver.junctionGraph.separators += separator02
    clique012.adjacencyList += clique023 -> separator02
    clique023.adjacencyList += clique012 -> separator02
    solver.dfsUpdate(clique012, solver.junctionGraph.cliques.clone(), mutable.Set[JunctionGraph.Separator]())
    solver.getTotalDistribution
    clique012.distribution.indices.foreach(marginalVariablesValues => assertApprox(
      clique012.distribution(marginalVariablesValues),
      IPFUtils.getMarginalProbability(4, solver.totalDistribution, 7, marginalVariablesValues)
    ))
    clique023.distribution.indices.foreach(marginalVariablesValues => assertApprox(
      clique023.distribution(marginalVariablesValues),
      IPFUtils.getMarginalProbability(4, solver.totalDistribution, 13, marginalVariablesValues)
    ))
    separator02.distribution.indices.foreach(marginalVariablesValues => assertApprox(
      separator02.distribution(marginalVariablesValues),
      IPFUtils.getMarginalProbability(4, solver.totalDistribution, 5, marginalVariablesValues)
    ))
  }

  @Test
  def testSolve6D(): Unit = {
    val randomGenerator = new Random()
    val data: Array[Double] = Array.fill(1 << 6)(0)
    (0 until 1 << 6).foreach(i => data(i) = randomGenerator.nextInt(100))

    val solver = new EffectiveIPFSolver(6)
    val marginalDistributions: Map[Seq[Int], Array[Double]] =
      Seq(Seq(0,1), Seq(1,2), Seq(2,3), Seq(0,3,4), Seq(4,5)).map(marginalVariables =>
        marginalVariables -> IPFUtils.getMarginalDistribution(6, data, marginalVariables.size, BitUtils.SetToInt(marginalVariables))
      ).toMap

    marginalDistributions.foreach { case (marginalVariables, clustersDistribution) =>
      solver.add(BitUtils.SetToInt(marginalVariables), clustersDistribution)
    }

    solver.solve()

    marginalDistributions.foreach { case (marginalVariables, marginalDistribution) =>
      marginalDistribution.indices.foreach(marginalVariablesValues => {
        assertApprox(
          marginalDistribution(marginalVariablesValues),
          IPFUtils.getMarginalProbability(6, solver.totalDistribution, BitUtils.SetToInt(marginalVariables), marginalVariablesValues)* solver.normalizationFactor
        )
        println()
      })
    }

  }

  private def assertApprox: (Double, Double) => Unit = (a, b) => assert((a - b).abs < eps)
}
