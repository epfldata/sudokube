package core.solver.iterativeProportionalFittingSolver

import util.{BitUtils, Profiler}

import scala.collection.mutable

/**
 * The IPF with loopy scaling updates.
 * The current version is the worst one with the smallest possible cliques (i.e., one cluster per clique).
 * TODO: delete printing statements for testing/demo
 * @author Zhekai Jiang
 * @param querySize Total number of dimensions queried.
 * @param solverName The name of the solver, "Worst Loopy IPF",
 *                   to be used in the messages to be printed to the console and as part of the names in the profiler.
 */
class WorstLoopyIPFSolver(override val querySize: Int, override val solverName: String = "Worst Loopy IPF")
  extends LoopyIPFSolver(querySize, solverName) {

  /**
   * Solve for the total distribution.
   * @return Solution as a one-dimensional array (values encoded as bits of 1 in index).
   */
  override def solve(): Array[Double] = {
    Profiler(s"$solverName Junction Graph Construction") {
      constructGraphicalModel()
      constructJunctionGraph()
    }

    junctionGraph.printAllCliquesAndSeparators()

    verifyJunctionGraph() // for testing purpose only

    val totalNumUpdates = junctionGraph.cliques.foldLeft(0)((acc, clique) => acc + clique.N * clique.clusters.size)
    println(s"\t\t\t$solverName number of updates per iteration (sum of |C|*2^|alpha| across all cliques): $totalNumUpdates")

    var totalDelta: Double = 0
    var numIterations: Int = 0

    Profiler(s"$solverName Iterations") {
      do {
        numIterations += 1
        totalDelta = iterativeUpdate() // Start with any clique
        verifyReducedFormula()
        verifyJunctionGraph()
        verifyEqualToCuboids()
        println(s"\t\t\tSum of distribution: ${getTotalDistribution.sum}") // this shows that the probabilities do not even sum up to 1
      } while (totalDelta >= convergenceThreshold * (1 << querySize))
    }
    println(s"\t\t\t$solverName number of iterations $numIterations")

    Profiler(s"$solverName Solution Derivation") {
      getTotalDistribution
      getSolution
    }

    solution
  }

  /**
   * Construct junction graph, in the way where each cluster is in a distinct clique.
   */
  def constructJunctionGraph(): Unit = {
    clusters.foreach(cluster => junctionGraph.cliques += new JunctionGraph.Clique(cluster.variables, mutable.Set[Cluster](cluster)))
    constructSeparators()
  }

  /**
   * Methods below are for testing/verification only
   */
  def verifyJunctionGraph(): Unit = {
    clusters.foreach(cluster => assert(junctionGraph.cliques.exists(_.clusters.contains(cluster))))
    junctionGraph.cliques.foreach(clique => clique.adjacencyList.foreach { case (_, separator) => assert(junctionGraph.separators.contains(separator)) })
    junctionGraph.separators.foreach(separator => assert(junctionGraph.cliques.exists(clique => clique.adjacencyList.exists { case (_, separator2) => separator2 == separator })))
    (0 until querySize).foreach(node => verifyConnectednessCondition(node))
  }

  def verifyEqualToCuboids(): Unit = {
    junctionGraph.cliques.foreach(clique => assertArrayApprox(clique.clusters.head.distribution, clique.distribution))
    junctionGraph.separators.foreach(separator => {
      val marginalDistributionFromClique1 = Array.fill(1 << separator.numVariables)(0.0)
      IPFUtils.getMarginalDistributionFromTotalDistribution(
        separator.clique1.numVariables, separator.clique1.distribution,
        IPFUtils.getVariableIndicesWithinVariableSubset(separator.variables, separator.clique1.variables), marginalDistributionFromClique1
      )
      val marginalDistributionFromClique2 = Array.fill(1 << separator.numVariables)(0.0)
      IPFUtils.getMarginalDistributionFromTotalDistribution(
        separator.clique2.numVariables, separator.clique2.distribution,
        IPFUtils.getVariableIndicesWithinVariableSubset(separator.variables, separator.clique2.variables), marginalDistributionFromClique2
      )
      assert(isArrayApprox(marginalDistributionFromClique1, separator.distribution)
        || isArrayApprox(marginalDistributionFromClique2, separator.distribution))
    })
  }


  def verifyConnectednessCondition(node: Int): Unit = {
    val remainingCliques = junctionGraph.cliques.filter(clique => (clique.variables & (1 << node)) != 0)
    val remainingSeparators = junctionGraph.separators.filter(separator => (separator.variables & (1 << node)) != 0)
    assert(remainingSeparators.size == remainingCliques.size - 1)
    dfsTraverse(remainingCliques.head, remainingCliques, remainingSeparators)
    assert(remainingCliques.isEmpty)
    assert(remainingSeparators.isEmpty)
    junctionGraph.cliques.foreach(clique => assertApprox(1.0, clique.distribution.sum))
    junctionGraph.separators.foreach(separator => {
      assert(separator.numVariables == IPFUtils.getNumOnesInBinary(separator.variables))
      assertApprox(1.0, separator.distribution.sum)
      assert((separator.variables & separator.clique1.variables) == separator.variables)
      assert((separator.variables & separator.clique2.variables) == separator.variables)
    })
  }

  def dfsTraverse(clique: JunctionGraph.Clique, remainingCliques: mutable.Set[JunctionGraph.Clique], remainingSeparators: mutable.Set[JunctionGraph.Separator]): Unit = {
    remainingCliques -= clique
    clique.adjacencyList.foreach { case (destination, separator) =>
      if (remainingSeparators.contains(separator) && remainingCliques.contains(destination)) {
        remainingSeparators -= separator
        dfsTraverse(destination, remainingCliques, remainingSeparators)
      }
    }
  }

  def verifyReducedFormula(): Unit = {
    getTotalDistribution
    (0 until 1 << querySize).foreach(allVariablesValues => {
      val calculatedValue = totalDistribution(allVariablesValues)
      junctionGraph.separators.foreach(separator => assertApprox(
        IPFUtils.getMarginalProbability(separator.clique1.numVariables, separator.clique1.distribution, IPFUtils.getVariableIndicesWithinVariableSubset(separator.variables, separator.clique1.variables), BitUtils.projectIntWithInt(allVariablesValues, separator.variables)),
        IPFUtils.getMarginalProbability(separator.clique2.numVariables, separator.clique2.distribution, IPFUtils.getVariableIndicesWithinVariableSubset(separator.variables, separator.clique2.variables), BitUtils.projectIntWithInt(allVariablesValues, separator.variables))
      ))
      val presumedNumerator = junctionGraph.cliques.foldLeft(1.0)((acc, clique) => acc * clique.distribution(BitUtils.projectIntWithInt(allVariablesValues, clique.variables)))
      val presumedDenominator = junctionGraph.separators.foldLeft(1.0)((acc, separator) => acc * IPFUtils.getMarginalProbability(separator.clique1.numVariables, separator.clique1.distribution, IPFUtils.getVariableIndicesWithinVariableSubset(separator.variables, separator.clique1.variables), BitUtils.projectIntWithInt(allVariablesValues, separator.variables)))
      val presumedValue = if (presumedDenominator == 0) {
        assert(presumedNumerator < 1e-10)
        0
      } else presumedNumerator / presumedDenominator
      if ((presumedValue - calculatedValue).abs / presumedValue > 1e-3) {
        println(s"\t\t\tProbability mismatch with presumed value: expected $presumedValue, got $calculatedValue")
      }
    })
  }

  private def assertApprox: (Double, Double) => Unit = (a, b) => {
    assert((a.abs < 1e-5 && b.abs < 1e-5) || ((a - b) / a).abs < 1e-3)
  }
  private def isArrayApprox(a: Array[Double], b: Array[Double]): Boolean = {
    assert(a.length == b.length)
    a.indices.forall(i => (a(i).abs < 1e-5 && b(i).abs < 1e-5) || ((a(i) - b(i)) / a(i)).abs < 1e-3)
  }
  private def assertArrayApprox(a: Array[Double], b: Array[Double]): Unit = {
    assert(a.length == b.length)
    a.indices.foreach(i => { assertApprox(a(i), b(i)) })
  }
}
