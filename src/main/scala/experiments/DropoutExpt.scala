package experiments

import core.DataCube
import core.solver.SolverTools
import core.solver.SolverTools.entropy
import core.solver.iterativeProportionalFittingSolver.IPFUtils.{getMarginalDistributionFromTotalDistribution, getVariableIndicesWithinVariableSubset, isVariableContained}
import core.solver.iterativeProportionalFittingSolver._
import core.solver.moment.{CoMoment5Solver, Moment1Transformer}
import util.{BitUtils, Profiler}

import scala.collection.mutable

/**
 * The experiment to drop out one cuboid at a time and record the resulting time, error, and entropy at each step for both IPF and moment solvers.
 * @author Zhekai Jiang
 * @param ename2 As experiment name.
 * @param policy Dropout policy, either "Dimension", "Normalized-Entropy" or "InvNormalized-Entropy".
 */
class DropoutExpt(ename2: String = "", policy: String) extends Experiment("ipf-moment", ename2, "dropout-ipf-moment") {

  // To see the moments lost with each dropout
  var prevMomentKnownSet: collection.mutable.BitSet = null
  var prevMoments: Array[Double] = null

  fileout.println(
    "TrueEntropy, DroppedCube, DroppedCubeDim, DroppedCubeEntropy, DroppedCubeMaxPossibleEntropy, DroppedCubeNormalizedEntropy, "
      + "RemCoverage, VarCoverage, NumCliques, NumEntries, "
      + "SolveTime(us), Err, Entropy, "
      + "SolveTimeWithout1D(us), ErrWithout1D, EntropyWithout1D, " // Without 1-dimensional marginals for variables that are not covered by cuboids
      + "NumLostMoments, "
      + "LostMomentsDistribution_<-1, -1_-1E-1, -1E-1_-1E-2, -1E-2_-1E-3, -1E-3_-1E-4, -1E-4_-1E-5, -1E-5_-1E-6, -1E-6_0, "
      + "0_1E-6, 1E-6_1E-5, 1E-5_1E-4, 1E-4_1E-3, 1E-3_1E-2, 1E-2_1E-1, 1E-1_1, >1, "
      + "MomentSolveTime(us), MomentErr, MomentEntropy"
  )


  /**
   * Prepare and fetch adapted from the moment solver.
   * @param solverName The name of the concrete solver, to be used in entries in the profiler.
   * @param dc DataCube object.
   * @param q Sequence of queried dimensions.
   * @return The fetched cuboids, the maximum dimensionality, and primary moments.
   */
  def momentPrepareFetch(solverName: String, dc: DataCube, q: IndexedSeq[Int]): (List[(Int, Array[Double])], Int, Seq[(Int, Double)]) = {
    val (l, primaryMoments) = Profiler(s"$solverName Prepare") {
      dc.index.prepareBatch(q) -> SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
    }
    val maxDimFetch = l.last.cuboidCost
    val fetched = Profiler(s"$solverName Fetch") {
      l.map { pm => (pm.queryIntersection, dc.fetch2[Double](List(pm))) }
    }
    println(s"\t\t\tNumber of cubes fetched: ${fetched.length}, Cube sizes (counts): " + s"${
      fetched.map{ case (bits, _) => BitUtils.sizeOfSet(bits) }
        .groupBy(identity).mapValues(_.size).toList.sorted
        .map { case (cubeSizes, count) => s"$cubeSizes ($count)" }.mkString(", ")
    }")
    (fetched, maxDimFetch, primaryMoments)
  }

  /**
   * Solve with the effective version of IPF (with junction tree).
   * We have already dropped clusters / cuboids we wanted so we simply run the effective IPF normally.
   * @param clusters Set of remaining clusters.
   * @param q Sequence of queried dimensions.
   * @param trueResult True result of the query.
   * @param dcname Name of the data cube.
   * @param oneDimMarginals The one dimensional marginal distributions of each variable,
   *                        as an array, indexed by the variable number, of an array of the 1-dimensional probability distribution (2 entries only).
   *                        For the dropout experiment to test cases where some variables are no longer covered by any remaining cuboid.
   *                        Optional, set to null by default meaning there is no given one dimensional marginal.
   * @param grandTotal The sum of all elements (which can also be thought of as normalization factor / moment of the empty set).
   *                   For the dropout experiment to test cases where there is no cuboid left.
   *                   Optional, set to 1.0 by default meaning it is not given.
   * @return The solver object.
   */
  def effectiveIPF_solve(clusters: mutable.Set[Cluster], q: IndexedSeq[Int], trueResult: Array[Double], dcname: String,
                         oneDimMarginals: Array[Array[Double]] = null, grandTotal: Double = 1.0): Unit = {
    println(s"\tRunning " + (if (oneDimMarginals == null) "without " else "with ") + "1D marginals")
    Profiler.resetAll()

    val solver = Profiler("Effective IPF Constructor + Add + Solve") {
      val solver = Profiler("Effective IPF Constructor") { new EffectiveIPFSolver(q.length) }
      Profiler("Effective IPF Add One Dimensional Marginals and Grand Total") {
        //TODO: Use the 1D marginals pre-computed and returned by prepare
        solver.oneDimMarginals = oneDimMarginals
        solver.normalizationFactor = grandTotal
      }
      Profiler("Effective IPF Add") { clusters.foreach { case Cluster(bits, array) => solver.add(bits, array) } }
      Profiler("Effective IPF Solve") { solver.solve() }
      solver
    }
    val solveTime = Profiler.durations("Effective IPF Solve")._2 / 1000
    val error = SolverTools.error(trueResult, solver.getSolution)
    val solutionEntropy = entropy(solver.getSolution)
    println(s"\t\tSolve time: $solveTime, solution error $error, entropy $solutionEntropy")
    if (oneDimMarginals != null) { // The first one, with one dimensional marginals
      fileout.print(s", ${solver.junctionGraph.cliques.size}, ${solver.junctionGraph.cliques.toList.map(1 << _.numVariables).sum}" +
        s", $solveTime, $error, $solutionEntropy")
    } else { // Continuing, without one dimensional marginals
      fileout.print(s", $solveTime, $error, $solutionEntropy")
    }
  }

  /**
   * Solve with the moment solver with some cuboids dropped.
   * @param clusters Set of remaining clusters. Here "cluster" was supposed to be for IPF but can be added to the moment solver as cuboids.
   * @param q Sequence of queried dimensions.
   * @param trueResult True result of the query.
   * @param dcname Name of the data cube.
   * @param primaryMoments Sequence of primary moments fetched.
   */
  def moment_solve(clusters: mutable.Set[Cluster], q: IndexedSeq[Int], trueResult: Array[Double], dcname: String,
                   primaryMoments: Seq[(Int, Double)]): Unit = {
    println(s"\tRunning moment solver")
    Profiler.resetAll()

    val solver = Profiler("Moment Solve") {
      val solver = Profiler("Moment Constructor") {
        new CoMoment5Solver(q.length, true, Moment1Transformer[Double](), primaryMoments)
      }
      Profiler("Moment Add") {
        clusters.foreach { case Cluster(bits, array) => solver.add(bits, array) }
      }

      // Print moments that are lost with the current dropout, as a "histogram"
      if (prevMomentKnownSet == null) { // first run with all cuboids, no moment lost
        fileout.print(", , , , , , , , , , , , , , , , , ")
      } else {
        val lostMomentIndices: collection.mutable.BitSet = prevMomentKnownSet -- solver.knownSet
        val numLostMoments = lostMomentIndices.size
        val lostNormalizedMoments = lostMomentIndices.map(prevMoments(_))

        println(s"\t\tLost $numLostMoments moments")
        fileout.print(s", $numLostMoments")
        printAndWriteLostMomentsHistogram(lostNormalizedMoments, numLostMoments)
      }

      // Update
      prevMomentKnownSet = solver.knownSet.clone()
      prevMoments = Array.fill(solver.moments.length)(0.0)
      solver.momentsToAdd.foreach { case (i, m) =>
        val mu = m
        prevMoments(i) = mu / solver.moments(0)
      }

      Profiler("Moment FillMissing") {
        solver.fillMissing()
      }
      Profiler("Moment ReverseTransform") {
        solver.solve()
      }
      solver
    }

    val solveTime = Profiler.durations("Moment Solve")._2 / 1000
    val error = SolverTools.error(trueResult, solver.solution)
    val solutionEntropy = entropy(solver.solution)
    println(s"\t\tSolve time: $solveTime, solution error $error, entropy $solutionEntropy")
    fileout.println(s", $solveTime, $error, $solutionEntropy")
  }

  /**
   * Print to console and write to fileout the fractions of lost moments, normalized by m_{empty set}, in the ranges of
   * (-inf, -1), [-1, -1e-1), [-1e-1, -1e-2), [-1e-2, -1e-3), [-1e-3, -1e-4), [-1e-4, -1e-5), [-1e-5, -1e-6), [-1e-6, 0),
   * [0, 1e-6), [1e-6, 1e-5), [1e-5, 1e-4), [1e-4, 1e-3), [1e-3, 1e-2), [1e-2, 1e-1), [1e-1, 1), (1, +inf)
   * @param lostNormalizedMoments Set of lost moments normalized by m_{empty set}.
   * @param numLostMoments Total number of moments lost by the dropout.
   */
  def printAndWriteLostMomentsHistogram(lostNormalizedMoments: mutable.Set[Double], numLostMoments: Int): Unit = {
    val numMomentsSmallerThanNeg1 = lostNormalizedMoments.count(_ < -1)
    println(s"\t\t\t< -1 : ${numMomentsSmallerThanNeg1.toDouble / numLostMoments}")
    fileout.print(s", ${numMomentsSmallerThanNeg1.toDouble / numLostMoments}")

    Seq[(Double, Double)](
      (-1, -1e-1), (-1e-1, -1e-2), (-1e-2, -1e-3), (-1e-3, -1e-4), (-1e-4, -1e-5), (-1e-5, -1e-6), (-1e-6, 0),
      (0, 1e-6), (1e-6, 1e-5), (1e-5, 1e-4), (1e-4, 1e-3), (1e-3, 1e-2), (1e-2, 1e-1), (1e-1, 1)
    ).foreach{ case (lowerBound, upperBound) =>
      val numMomentsInRange = lostNormalizedMoments.count(moment => lowerBound <= moment && moment < upperBound)
      println(s"\t\t\t[$lowerBound to $upperBound) : ${numMomentsInRange.toDouble / numLostMoments}")
      fileout.print(s", ${numMomentsInRange.toDouble / numLostMoments}")
    }

    val numMomentsGreaterThan1 = lostNormalizedMoments.count(_ >= 1)
    println(s"\t\t\t>= 1 : ${numMomentsGreaterThan1.toDouble / numLostMoments}")
    fileout.print(s", ${numMomentsGreaterThan1.toDouble / numLostMoments}")
  }


  /**
   * Run experiment with the specified dropout policy.
   * @param dc The DataCube object.
   * @param dcname Name of the data cube.
   * @param qu Unsorted sequence of queries dimensions.
   * @param trueResult The true result of the query.
   * @param output Whether to output.
   * @param qname Name of the query.
   * @param sliceValues For slicing.
   */
  def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: IndexedSeq[Int]): Unit = {
    val q = qu.sorted
    println(s"$policy-Based")

    val (fetched, _, primaryMoments) = momentPrepareFetch("Effective IPF", dc, q)
    val clusters = mutable.Set[Cluster]() ++ fetched.map { case (bits, array) => Cluster(bits, array) } // unnormalized here

    val clustersQueue: mutable.PriorityQueue[Cluster] =
      if (policy == "Dimension")
        mutable.PriorityQueue[Cluster]()(Ordering.by(cluster =>
          - cluster.numVariables // low dimensionality to high dimensionality in case of tie
        )) ++ clusters
      else if (policy == "Normalized-Entropy")
        mutable.PriorityQueue[Cluster]()(Ordering.by(cluster => (
          normalizedEntropy(cluster), // high to low entropy
          - cluster.numVariables // low dimensionality to high dimensionality in case of tie
        ))) ++ clusters
      else
        mutable.PriorityQueue[Cluster]()(Ordering.by(cluster => (
          - normalizedEntropy(cluster), // low to high entropy
          cluster.numVariables // high dimensionality to low dimensionality in case of tie
        ))) ++ clusters
    val skippedClusters = mutable.Queue[Cluster]() // Skip when cuboid contains some variables not covered by any other cuboid.
    var totalCoverage = clusters.toList.map(_.numVariables).sum
      // Define by the sum of number of variables across all cuboids, showing how "fully" the queried variables are covered.

    // Remember all 1-dimensional marginals
    val grandTotal = clusters.head.distribution.sum
    val oneDimMarginals: Array[Array[Double]] = get1DMarginals(q.size, clusters, grandTotal)

    val variableCoverage = q.indices.map(variable =>
        clusters.count(cluster => isVariableContained(variable, cluster.variables))
      ).toBuffer // Number of clusters covering each variable

    println("\tFull cuboids")
    fileout.print(s"${entropy(trueResult)}, , , , , , $totalCoverage, ${variableCoverage.mkString(":")}")
    effectiveIPF_solve(clusters, q, trueResult, dcname, oneDimMarginals, grandTotal)
    fileout.print(s", , , ")
    moment_solve(clusters, q, trueResult, dcname, primaryMoments)

    var cuboidIndex = 0

    def dropCluster(cluster: Cluster): Unit = {
      cuboidIndex += 1
      totalCoverage -= cluster.numVariables
      BitUtils.IntToSet(cluster.variables).foreach(variableCoverage(_) -= 1)
      clusters -= cluster
      println(s"\tDropped cuboid $cuboidIndex/${fetched.size}: size ${cluster.numVariables}, " +
        s"entropy ${entropy(cluster.distribution)}, normalized entropy ${normalizedEntropy(cluster)}, " +
        s"variables ${BitUtils.IntToSet(cluster.variables).mkString(":")}"
      )
      fileout.print(s"${entropy(trueResult)}, ${BitUtils.IntToSet(cluster.variables).mkString(":")}, ${cluster.numVariables}, " +
        s"${entropy(cluster.distribution)}, ${-math.log(1.0 / (1 << cluster.numVariables))}, ${normalizedEntropy(cluster)}, " +
        s"$totalCoverage, ${variableCoverage.mkString(":")}"
      )
    }

    while (clustersQueue.nonEmpty) {
      val cluster = clustersQueue.dequeue()
      if (BitUtils.IntToSet(cluster.variables).forall(variableCoverage(_) > 1)) {
        dropCluster(cluster)
        effectiveIPF_solve(clusters, q, trueResult, dcname, oneDimMarginals, grandTotal)
        fileout.print(s", , , ")
        moment_solve(clusters, q, trueResult, dcname, primaryMoments)
      } else { // There exists a variable that will no longer be covered without this cluster.
        skippedClusters += cluster
      }
    }

    println("Starting to drop skipped clusters")
    while (skippedClusters.nonEmpty) {
      val cluster = skippedClusters.dequeue()
      dropCluster(cluster)
      effectiveIPF_solve(clusters, q, trueResult, dcname, oneDimMarginals, grandTotal)
      effectiveIPF_solve(clusters, q, trueResult, dcname, null, grandTotal)
      moment_solve(clusters, q, trueResult, dcname, primaryMoments)
    }
  }

  /**
   * Get the one-dimensional marginal distributions for all variables.
   * @param querySize The number of dimensions queried.
   * @param clusters Available clusters / cuboids.
   * @param normalizationFactor The sum of all entries, the grand total.
   * @return The one dimensional marginal distributions of each variable,
   *         as an array, indexed by the variable number, of an array of the 1-dimensional probability distribution (2 entries only).
   *         For the dropout experiment to test cases where some variables are no longer covered by any remaining cuboid.
   */
  def get1DMarginals(querySize: Int, clusters: mutable.Set[Cluster], normalizationFactor: Double): Array[Array[Double]] = {
    val oneDimMarginals: Array[Array[Double]] = new Array[Array[Double]](querySize)
    (0 until querySize).foreach(variable => {
      clusters.find(cluster => isVariableContained(variable, cluster.variables)) match {
        case Some(coveringCluster) =>
          oneDimMarginals(variable) = getMarginalDistributionFromTotalDistribution(
            coveringCluster.numVariables,
            coveringCluster.distribution.map(_ / normalizationFactor), getVariableIndicesWithinVariableSubset(1 << variable, coveringCluster.variables)
          )
        case None => oneDimMarginals(variable) = Array.fill(2)(0.5) // Assume uniform distribution
      }
    })
    oneDimMarginals
  }


  /**
   * Helper function to compute the ratio of the entropy of a cluster relative to the maximum possible entropy at its dimensionality.
   * @param cluster The cluster.
   * @return The ratio of the entropy of a cluster relative to the maximum possible entropy at its dimensionality.
   */
  def normalizedEntropy(cluster: Cluster): Double = entropy(cluster.distribution) / (-math.log(1.0 / (1 << cluster.numVariables)))
}
