package experiments

import backend.{Backend, Payload}
import combinatorics.Combinatorics
import core.{DataCube, PartialDataCube}
import core.solver.SolverTools
import core.solver.SolverTools.entropy
import core.solver.iterativeProportionalFittingSolver._
import core.solver.moment.{CoMoment5Solver, Moment1Transformer}
import frontend.generators.CubeGenerator
import planning.NewProjectionMetaData
import util.{BitUtils, Profiler}

import scala.collection.mutable

/**
 * The experiment to measure the performance of different score functions.
 * We record resulting time, error, entropy, and various score functions, averaged over several runs.
 * In the experiment, we generate run-many queries and run them against different materialization dimensions
 * to record the aforementioned values.
 * Reported will be the average over all runs.
 * We use as solvers EffectiveIPF and CoMoment5.
 *
 * @note Must be called via "runAll".
 *
 * @param ename2 As experiment name.
 *
 * @author Thomas Depian
 */
class ScoringFunctionsPerformanceExpt(ename2: String = "")(implicit shouldRecord: Boolean) extends Experiment("scoring", ename2, "scoring-performance") {

  fileout.println(
    "k, EntropyEffectiveIPF, TimeEffectiveIPF, ErrorEffectiveIPF, "
      + "EntropyMoment, TimeMoment, ErrorMoment, "
      + "PowerScore, WeightedPowerScore, "
      + "DiffToQueryScore, InclusionExclusionScore, UncoveredScore, "
      + "MaterializedCuboids, UsedCuboids, IntersectionSize"
  )

  // used to captures the different measurements in order to take the average afterwards
  var ipfSolutionEntropy = 0.0
  var ipfError = 0.0
  var ipfSolveTime = 0.0
  var momentSolutionEntropy = 0.0
  var momentError = 0.0
  var momentSolveTime = 0.0
  var scorePower = 0.0
  var scoreWeightedPower = 0.0
  var scoreDiffQuery = 0.0
  var scoreInclusionExclusion = 0.0
  var scoreUncovered = 0.0
  var usedCuboids = 0.0
  var intersectionSizes = ""

  /**
   * Performs the actual fetch operation.
   * Code adapted from the momentPrepareFetch-method from the DropoutExpt.
   * @param l Metadata on the projection.
   * @param dc Datacube from which we want to fetch.
   * @return List of fetched cuboids. Each entry is a tuple containing the dimensions and the actual data.
   */
  def actualFetch(l: Seq[NewProjectionMetaData], dc: DataCube): (List[(Int, Array[Double])], Int) = {
    val maxDimFetch = l.last.cuboidCost
    val fetched = Profiler(s"Scoring Fetch") {
      l.map { pm => (pm.queryIntersection, dc.fetch2[Double](List(pm))) }
    }
    println(s"\t\t\tNumber of cubes fetched: ${fetched.length}, Cube sizes (counts): " + s"${
      fetched.map{ case (bits, _) => BitUtils.sizeOfSet(bits) }
        .groupBy(identity).mapValues(_.size).toList.sorted
        .map { case (cubeSizes, count) => s"$cubeSizes ($count)" }.mkString(", ")
    }")
    (fetched.toList, maxDimFetch)
  }

  /**
   * Prepare and fetch adapted from the momentPrepareFetch-method from the DropoutExpt.
   * @param dc DataCube object.
   * @param query Sequence of queried dimensions.
   * @return The projection meta data, fetched cuboids, the maximum dimensionality, and primary moments.
   */
  def momentPrepareFetch(dc: DataCube, query: IndexedSeq[Int]): (Seq[NewProjectionMetaData], List[(Int, Array[Double])], Int, Seq[(Int, Double)]) = {
    val (l, primaryMoments) = Profiler(s"Scoring Prepare") {
      dc.index.prepareBatch(query) -> SolverTools.preparePrimaryMomentsForQuery[Double](query, dc.primaryMoments)
    }

    val (fetched, maxDimFetch) = actualFetch(l, dc)
    (l, fetched, maxDimFetch, primaryMoments)
  }

  /**
   * Solve with the effective version of IPF (with junction tree).
   * Adapted from the DropoutExpt-file.
   * Records the solve time, error and entropy of the reported solution.
   *
   * @param clusters Set of clusters.
   * @param query Sequence of queried dimensions.
   * @param trueResult True result of the query.
   */
  def effectiveIPF_solve(clusters: List[Cluster], query: IndexedSeq[Int], trueResult: Array[Double]): Unit = {
    println(s"\tRunning ipf solver")
    Profiler.resetAll()
    val solver = Profiler("Effective IPF Constructor + Add + Solve") {
      val solver = Profiler("Effective IPF Constructor") { new EffectiveIPFSolver(query.length) }
      Profiler("Effective IPF Add") { clusters.foreach { case Cluster(bits, array) => solver.add(bits, array) } }
      Profiler("Effective IPF Solve") { solver.solve() }
      solver
    }

    val solveTime = Profiler.durations("Effective IPF Solve")._2 / 1000
    val error = SolverTools.error(trueResult, solver.getSolution)
    val solutionEntropy = entropy(solver.getSolution)
    println(s"\t\tSolve time: $solveTime, solution error $error, entropy $solutionEntropy")
    ipfSolutionEntropy += solutionEntropy
    ipfError += error
    ipfSolveTime += solveTime
  }

  /**
   * Solve with the moment solver.
   * Adapted from the DropoutExpt-file.
   * Records the solve time, error and entropy of the reported solution.
   *
   * @param clusters Set of clusters. Here "cluster" was supposed to be for IPF but can be added to the moment solver as cuboids.
   * @param primaryMoments Sequence of primary moments fetched.
   * @param query Sequence of queried dimensions.
   * @param trueResult True result of the query.
   */
  def moment_solve(clusters: List[Cluster], primaryMoments: Seq[(Int, Double)], query: IndexedSeq[Int], trueResult: Array[Double]): Unit = {
    println(s"\tRunning moment solver")
    Profiler.resetAll()


    val solver = Profiler("Moment Solve") {
      val s = Profiler("Moment Constructor") {
        new CoMoment5Solver(query.length, true, Moment1Transformer[Double](), primaryMoments)
      }
      Profiler("Moment Add") {
        clusters.foreach { case Cluster(bits, array) => s.add(bits, array) }
      }
      Profiler("Moment FillMissing") {
        s.fillMissing()
      }
      Profiler("Moment ReverseTransform") {
        s.solve()
      }
      s
    }

    val solveTime = Profiler.durations("Moment Solve")._2 / 1000
    val error = SolverTools.error(trueResult, solver.solution)
    val solutionEntropy = entropy(solver.solution)
    println(s"\t\tSolve time: $solveTime, solution error $error, entropy $solutionEntropy")
    momentSolutionEntropy += solutionEntropy
    momentError += error
    momentSolveTime += solveTime
  }

  /**
   * Compute the scores for the set of fetched clusters.
   * Records the various scores.
   *
   * @param clusters The fetched clusters.
   * @param q: The dimensionality of the query.
   * @param query: The actual query.
   */
  def computeScores(clusters: List[Cluster], q: Int, query: IndexedSeq[Int]): Unit = {
    val queryAsSet = query.toSet

    scorePower += clusters.map(c => math.pow(2, c.numVariables)).sum
    scoreWeightedPower += clusters.map(c => c.numVariables * math.pow(2, c.numVariables)).sum
    scoreDiffQuery += clusters.map(c => math.pow(2, q) - math.pow(2, q - c.numVariables)).sum

    val scoreUncoveredCorrectionTerm = (1 to q).map(i => Combinatorics.comb(q, i).toDouble * math.pow(2, i)).sum
    scoreUncovered += scoreUncoveredCorrectionTerm - queryAsSet.subsets().filter(s => s.nonEmpty).filterNot(s => clusters.exists(c => s.subsetOf(BitUtils.IntToSet(c.variables).toSet))).map(c => math.pow(2, c.size)).sum

    val powerSets = mutable.Set[Set[Int]]()
    clusters.map(c => BitUtils.IntToSet(c.variables).toSet.subsets()).foreach(s => s.foreach(s2 => powerSets.add(s2)))
    scoreInclusionExclusion += powerSets.size
  }

  /**
   * Resets all variables that capture the measurements.  */
  def resetEverything(): Unit = {
    ipfSolutionEntropy = 0.0
    ipfError = 0.0
    ipfSolveTime = 0.0
    momentSolutionEntropy = 0.0
    momentError = 0.0
    momentSolveTime = 0.0
    scorePower = 0.0
    scoreWeightedPower = 0.0
    scoreDiffQuery = 0.0
    scoreInclusionExclusion = 0.0
    scoreUncovered = 0.0
    usedCuboids = 0.0
    intersectionSizes = ""
  }

  /**
   * Run one concrete experiment.
   * @param dc The DataCube object.
   * @param dcname Name of the data cube (not used).
   * @param qu: The query.
   * @param trueResult The true result of the query.
   * @param output Whether to output.
   * @param qname The name of the query (not used).
   * @param sliceValues Values for the slicing (not used).
   */
  def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean, qname: String, sliceValues: Seq[(Int, Int)]): Unit = {
    val query = qu.sorted
    val q = query.size

    // in this setup, we assume that the prepare statement from the SudoKube system DOES NOT eliminate subsumed cuboids
    // fetch without subsumption
    val (cubeMetaData, fetched, _, primaryMoments) = momentPrepareFetch(dc, query)
    val clusters = List[Cluster]() ++ fetched.map { case (bits, array) => Cluster(bits, array) }

    println("\tScores")
    computeScores(clusters, q, query)

    // eliminate now redundant stuff for the solvers
    val noRed = dc.index.eliminateRedundant(cubeMetaData, dc.index.n_bits - 1)
    val (fetchedNoRed, _) = actualFetch(noRed, dc)
    val clustersNoRed = List[Cluster]() ++ fetchedNoRed.map { case (bits, array) => Cluster(bits, array) }

    intersectionSizes += "//" + clustersNoRed.map(c => c.numVariables).mkString(";")

    println("\tIPF")
    effectiveIPF_solve(clustersNoRed, query, trueResult)
    println("\tMoment")
    moment_solve(clustersNoRed, primaryMoments, query, trueResult)
    println("\tCuboid Info")
    usedCuboids += clustersNoRed.size
  }


  /**
   * Run one concrete experiment.
   * We run the query against each set of materialized cuboids.
   * @param cg Generator of the base cuboid.
   * @param d0 Sparsity of the base cuboid (there are pow(2, d0)-many non-zero entries).
   * @param b Materialization budget.
   * @param dcname Name of the datacube (not used).
   * @param qus Array of queries. Foreach query, we run one experiment (=run).
   * @param trueResults True result of the queries. The order must be identical to the one in qus, i.e., trueResults[i] = result(ques[i]).
   * @param output Whether it should output.
   * @param qname Name of the query (not used).
   * @param sliceValues Values for the slicing operation (not used).
   */
  def runAll(cg: CubeGenerator, d0: Int, b: Double,  dcname: String, qus: Array[IndexedSeq[Int]], trueResults: Array[Array[Double]], output: Boolean, qname: String, sliceValues: Seq[(Int, Int)])(implicit backend: Backend[Payload]): Unit = {
    val runs = qus.length
    (2 to d0).foreach(k => {
      println(s"Run for Dimensionality $k")
      fileout.print(k)
      resetEverything()
      val fullname = cg.inputname + s"_${b}_$k"
      val dc = PartialDataCube.load(fullname, cg.baseName)
      dc.loadPrimaryMoments(cg.baseName)
      qus.indices.foreach(r => {
        run(dc, dcname, qus(r), trueResults(r), output, qname, sliceValues)
      })
      fileout.println(s", ${ipfSolutionEntropy / runs}, ${ipfSolveTime / runs}, ${ipfError / runs}, ${momentSolutionEntropy / runs}, ${momentSolveTime / runs}, ${momentError / runs}, ${scorePower / runs}, ${scoreWeightedPower / runs}, ${scoreDiffQuery / runs}, ${scoreInclusionExclusion / runs}, ${scoreUncovered / runs}, ${dc.cuboids.length}, ${usedCuboids / runs}, $intersectionSizes")
      dc.cuboids.head.backend.reset
    })

    fileout.println("Queries:")
    qus.indices.foreach(r => {
      fileout.println(s"run = $r; query = ${qus(r).mkString(", ")}")
    })

  }
}
