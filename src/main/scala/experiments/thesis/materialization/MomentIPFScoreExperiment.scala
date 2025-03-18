package experiments.thesis.materialization

import backend.CBackend
import combinatorics.Combinatorics
import core.cube.OptimizedArrayCuboidIndex
import core.solver.SolverTools
import core.solver.SolverTools.entropy
import core.solver.iterativeProportionalFittingSolver.{Cluster, EffectiveIPFSolver}
import core.solver.moment.CoMoment5SolverDouble
import core.{DataCube, MaterializedQueryResult, PartialDataCube}
import experiments.{Experiment, ExperimentRunner}
import frontend.generators.{CubeGenerator, NYC, RandomCubeGenerator, SSBSample}
import planning.NewProjectionMetaData
import util.{BitUtils, Profiler}

import scala.collection.mutable

class MomentIPFScoreExperiment(ename2: String = "")(implicit timestampedFolder: String, numIters: Int) extends Experiment(ename2, s"moment-score", "thesis/materialization") {
  val header = "CubeName,RunID,Query,QSize," +
    "PrepareTime(us),FetchTime(us)," +
    "CM5SolveTime(us),IPFSolveTime(us)," +
    "DOF,CM5ErrL1,IPFErrL1," +
    "CM5Entropy,IPFEntropy," +
    "scoreMultisetPower,scorePower,scoreWeightedPower,scoreDiffToQuery,scoreUncovered,scoreIE"
  fileout.println(header)
  var runID = 0

  def actualFetch(l: Seq[NewProjectionMetaData], dc: DataCube): List[(Int, Array[Double])] = {
    val fetched = Profiler(s"Scoring Fetch") {
      l.map { pm => (pm.queryIntersection, dc.fetch2[Double](List(pm))) }
    }
    fetched.toList
  }
  /**
   * Compute the scores for the set of fetched clusters.
   * Records the various scores.
   *
   * @param clusters The fetched clusters.
   * @param q: The dimensionality of the query.
   * @param query: The actual query.
   */
  def computeScores(clusters: List[Cluster], q: Int, query: IndexedSeq[Int]) = {
    val queryAsSet = query.toSet

    val scorePower = clusters.map(c => math.pow(2, c.numVariables)).sum
    val scoreWeightedPower = clusters.map(c => c.numVariables * math.pow(2, c.numVariables)).sum
    val scoreDiffQuery = clusters.map(c => math.pow(2, q) - math.pow(2, q - c.numVariables)).sum

    val scoreUncoveredCorrectionTerm = (1 to q).map(i => Combinatorics.comb(q, i).toDouble * math.pow(2, i)).sum
    val scoreUncovered = scoreUncoveredCorrectionTerm - queryAsSet.subsets().filter(s => s.nonEmpty).filterNot(s => clusters.exists(c => s.subsetOf(BitUtils.IntToSet(c.variables).toSet))).map(c => math.pow(2, c.size)).sum

    val powerSets = mutable.Set[Set[Int]]()
    clusters.map(c => BitUtils.IntToSet(c.variables).toSet.subsets()).foreach(s => s.foreach(s2 => powerSets.add(s2)))
    val scoreInclusionExclusion = powerSets.size
    Vector(scorePower, scoreWeightedPower, scoreDiffQuery, scoreUncovered, scoreInclusionExclusion)
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
  def effectiveIPF_solve(clusters: List[Cluster], query: IndexedSeq[Int], trueResult: Array[Double]) = {
    Profiler.resetAll()
    val solver = Profiler("IPF SolveTotal") {
      val solver = Profiler.noprofile("Effective IPF Constructor") { new EffectiveIPFSolver(query.length) }
      Profiler.noprofile("Effective IPF Add") { clusters.foreach { case Cluster(bits, array) => solver.add(bits, array) } }
      Profiler.noprofile("Effective IPF Solve") { solver.solve() }
      solver
    }

    val solveTime = Profiler.getDurationMicro("IPF SolveTotal")
    val error = SolverTools.error(trueResult, solver.getSolution)
    val solutionEntropy = entropy(solver.getSolution)
    //println(s"\t\tSolve time: $solveTime, solution error $error, entropy $solutionEntropy")
    (solveTime, error, solutionEntropy)
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
  def moment_solve(clusters: List[Cluster], primaryMoments: Seq[(Int, Double)], query: IndexedSeq[Int], trueResult: Array[Double]) = {
    //println(s"\tRunning moment solver")
    Profiler.resetAll()


    val solver = Profiler("Moment SolveTotal") {
      val s = new CoMoment5SolverDouble(query.length, true, null, primaryMoments)
      clusters.foreach { case Cluster(bits, array) => s.add(bits, array) }
      s.fillMissing()
      s.solve()
      s
    }

    val solveTime = Profiler.getDurationMicro("Moment SolveTotal")
    val error = SolverTools.error(trueResult, solver.solution)
    val solutionEntropy = entropy(solver.solution)
    val dof = solver.dof
    //println(s"\t\tSolve time: $solveTime, solution error $error, entropy $solutionEntropy")
    (solveTime, error, solutionEntropy, dof)
  }

  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Seq()): Unit = {
    val query = qu.sorted
    val qsize = qu.size
    Profiler.resetAll()
    val primaryMoments = Profiler("Prepare") { SolverTools.preparePrimaryMomentsForQuery[Double](query, dc.primaryMoments) }
    val withRed = Profiler("Prepare") { dc.index.qproject(query, dc.index.n_bits - 1).sorted(NewProjectionMetaData.ordering) }
    //println("\tScores")
    //multiset power score does not use the clusters for query, but directly all cuboids from the index
    //FIXME: Assuming OptimizedArrayCuboidIndex is used
    val dcindex = dc.index.asInstanceOf[OptimizedArrayCuboidIndex]
    val scoreMultiSetPower = dc.index.map { cub =>
      val intersectionSize = dcindex.intersect(query, cub)._2.size
      1 << intersectionSize
    }.sum
    val scores = scoreMultiSetPower +: computeScores(withRed.toList.map { pm => Cluster(pm.queryIntersection, null) }, qsize, query)

    // eliminate now redundant stuff for the solvers
    val noRed = Profiler("Prepare") { dc.index.eliminateRedundant(withRed, dc.index.n_bits - 1) }

    //Add any missing 1-D marginals if any, for fair comparison for IPF as other solvers have separate access to them anyway
    val unionOfBits = noRed.map { _.queryIntersection }.reduce(_ | _)
    val total = primaryMoments.head._2
    val missing1Dmarginals = primaryMoments.tail.flatMap { case (h, m) =>
      if ((unionOfBits & h) == 0)
        Some(h -> Array(total - m, m))
      else None
    }

    val fetchedNoRed = Profiler("Fetch") { missing1Dmarginals ++ actualFetch(noRed, dc) }
    val clustersNoRed = List[Cluster]() ++ fetchedNoRed.map { case (bits, array) => Cluster(bits, array) }

    val prepareTime = Profiler.getDurationMicro("Prepare")
    val fetchTime = Profiler.getDurationMicro("Fetch")
    val (momentSolveTime, momentError, momentEntropy, dof) = moment_solve(clustersNoRed, primaryMoments, query, trueResult)
    val (ipfSolveTime, ipfError, ipfEntropy) = effectiveIPF_solve(clustersNoRed, query, trueResult)

    val row = s"$dcname,$runID,${qu.mkString(";")},$qsize," +
      s"$prepareTime,$fetchTime," +
      s"$momentSolveTime,$ipfSolveTime," +
      s"$dof,$momentError,$ipfError," +
      s"$momentEntropy,$ipfEntropy," +
      scores.mkString(",")

    fileout.println(row)
    runID += 1
  }
}

object MomentIPFScoreExperiment extends ExperimentRunner {
  implicit val be = CBackend.default
  def singlelevel(cg: CubeGenerator, b: Int, qs: Int)(implicit timestampedFolder: String, numIters: Int) = {
    val ename = s"${cg.inputname}_${b}_${qs}"
    val expt = new MomentIPFScoreExperiment(ename)
    val mqr = new MaterializedQueryResult(cg, false)
    val queries = mqr.loadQueries(qs).take(numIters)
    (2 to 20).foreach { k =>
      val dcname = s"${cg.inputname}_random_${b}_$k"
      val dc = PartialDataCube.load(dcname, cg.baseName)
      dc.loadPrimaryMoments(cg.baseName)
      if(dc.cuboids.length > 1) { //materialize at least one cuboid other than base
        queries.zipWithIndex.foreach { case (query, qidx) =>
          val trueResult = mqr.loadQueryResult(qs, qidx)
          expt.run(dc, dc.cubeName, query, trueResult)
        }
      }
    }
  }

  def main(args: Array[String]) = {
    val nyc = new NYC()
    val ssb = new SSBSample(20)
    val rand = new RandomCubeGenerator(100, 20)
    def func(param: String)(timestamp: String, numIters: Int) = {
      implicit val ni = numIters
      implicit val ts = timestamp
      val myargs = param.split("_")
      val cg = myargs(0) match {
        case "nyc" => nyc
        case "ssb" => ssb
        case "random" => rand
        case s => throw new IllegalArgumentException(s)
      }
      val b = myargs(1).toInt
      val qs = myargs(2).toInt
      singlelevel(cg, b, qs)
    }
    run_expt(func)(args)
  }
}