package experiments

import core.DataCube
import core.solver.SolverTools
import core.solver.SolverTools.{entropy, error}
import core.solver.iterativeProportionalFittingSolver.{EffectiveIPFSolver, VanillaIPFSolver}
import core.solver.moment.{CoMoment3Solver, CoMoment5Solver, CoMoment5SolverDouble, Moment1Transformer}
import core.solver.simple.{AverageSolver, OneDProductSolver}
import util.{BitUtils, Profiler}

class IPFMomentBatchExpt2(ename2: String = "")(implicit timestampedFolder: String) extends Experiment(s"ipf-moment-simple-batch", ename2, "ipf-expts") {
  var queryCounter = 0

  val header = "CubeName,Query,QSize,  " +
    "Prepare(us),Fetch(us),NumFetched,FetchedSizes,  " +
    "CM3Solve(us),CM5NoFixSolve(us),CM5WithFixSolve(us),CM5WithFixZeroesSolve(us),vIPFSolve(us),eIPFSolve(us),oneDSolve(us),avgSolve(us),  " +
    "CM3Error,CM5NoFixError,CM5WithFixError,CM5WithFixZeroesError,vIPFError,eIPFError,oneDError,avgError,  " +
    "CM3Entropy,CM5NoFixEntropy,CM5WithFixEntropy,vIPFEntropy,eIPFEntropy,oneDEntropy,avgEntropy,   " +
    "vIPFNumIterations,eIPFNumIterations,  " +
    "eIPFNumCliques,eIPFCliqueSizes,eIPFCliqueClusterSizes,  " +
    "DOF,unknownMuDist"
  fileout.println(header)

  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)]): Unit = {
    val q = qu.sorted
    Profiler.resetAll()
    val (fetched, pm, maxDimFetch) = prepareFetch(dc, q)

    val cm3cutoff = 17
    val CM3Solver = Profiler("CM3 Solve") {
      val s = new CoMoment3Solver[Double](q.length, true, Moment1Transformer(), pm)
      if (q.length < cm3cutoff) {
        fetched.foreach { case (bits, array) => s.add(bits, array) }
        s.fillMissing()
        s.solve()
      }
      s
    }
    val CM5SolverNoFix = Profiler("CM5NoFix Solve") {
      val s = new CoMoment5SolverDouble(q.length, true, Moment1Transformer(), pm)
      fetched.foreach { case (bits, array) => s.add(bits, array) }
      s.fillMissing()
      s.solve(false)
      s
    }
    val CM5SolverWithFix = Profiler("CM5WithFix Solve") {
      val s = new CoMoment5SolverDouble(q.length, true, Moment1Transformer(), pm)
      fetched.foreach { case (bits, array) => s.add(bits, array) }
      s.fillMissing()
      s.solve(true)
      s
    }

    val CM5SolverWithFixZeroes = Profiler("CM5WithFixZeroes Solve") {
      val s = new CoMoment5SolverDouble(q.length, true, Moment1Transformer(), pm, true)
      fetched.foreach { case (bits, array) => s.add(bits, array) }
      s.fillMissing()
      s.solve(true)
      s
    }

    val vIPFSolver = Profiler("VIPF Solve") {
      val s = new VanillaIPFSolver(q.length)
      fetched.foreach { case (bits, array) => s.add(bits, array) }
      s.solve()
      s
    }

    val eIPFSolver = Profiler("EIPF Solve") {
      val s = new EffectiveIPFSolver(q.length)
      fetched.foreach { case (bits, array) => s.add(bits, array) }
      s.solve()
      s
    }

    val oneDSolver = Profiler("OneD Solve") {
      val s = new OneDProductSolver(q.length, pm)
      s.solve()
      s
    }

    val avgSolver = Profiler("Avg Solve") {
      val s = new AverageSolver(q.length, pm)
      fetched.foreach { case (bits, array) => s.add(bits, array) }
      s.solve()
      s
    }

    val prepareTime = Profiler.getDurationMicro("Prepare")
    val fetchTime = Profiler.getDurationMicro("Fetch")

    val cm3SolveTime = if (q.length < cm3cutoff) Profiler.getDurationMicro("CM3 Solve") else " "
    val cm5NoFixSolveTime = Profiler.getDurationMicro("CM5NoFix Solve")
    val cm5WithFixSolveTime = Profiler.getDurationMicro("CM5WithFix Solve")
    val cm5WithFixZeroesSolveTime = Profiler.getDurationMicro("CM5WithFixZeroes Solve")
    val vIPFSolveTime = Profiler.getDurationMicro("VIPF Solve")
    val eIPFSolveTime = Profiler.getDurationMicro("EIPF Solve")
    val oneDSolveTime = Profiler.getDurationMicro("OneD Solve")
    val avgSolveTime = Profiler.getDurationMicro("Avg Solve")


    val cm3Entropy = if (q.length < cm3cutoff) entropy(CM3Solver.solution) else " "
    val cm5NoFixEntropy = entropy(CM5SolverNoFix.solution)
    val cm5WithFixEntropy = entropy(CM5SolverWithFix.solution)
    val vIPFEntropy = entropy(vIPFSolver.solution)
    val eIPFEntropy = entropy(eIPFSolver.solution)
    val oneDEntropy = entropy(oneDSolver.solution)
    val avgEntropy = entropy(avgSolver.solution)


    if (output) {

      val cm3Error = if (q.length < cm3cutoff) error(trueResult, CM3Solver.solution) else " "
      val cm5NoFixError = error(trueResult, CM5SolverNoFix.solution)
      val cm5WithFixError = error(trueResult, CM5SolverWithFix.solution)
      val cm5WithFixZeroesError = error(trueResult, CM5SolverWithFixZeroes.solution)
      val vIPFError = error(trueResult, vIPFSolver.solution)
      val eIPFError = error(trueResult, eIPFSolver.solution)
      val oneDError = error(trueResult, oneDSolver.solution)
      val avgError = error(trueResult, avgSolver.solution)


      val total = trueResult.sum
      val cliques = eIPFSolver.junctionGraph.cliques.toList
      val allmus = Moment1Transformer[Double]().getCoMoments(trueResult, CM5SolverWithFix.pmArray)
      val dof = CM5SolverWithFix.dof
      val unknownMus = allmus.indices.filter(i => !CM5SolverWithFix.knownSet.contains(i)).map(i => math.abs(allmus(i) / total))
      val countMax = 16
      val muCounts = Array.fill(countMax)(0)
      unknownMus.foreach { mu =>
        val log = -math.floor(math.log10(mu))
        val idx = if (log >= countMax) countMax - 1 else log.toInt
        muCounts(idx) += 1
      }
      val resultRow = s"$dcname,${qu.mkString(":")},${q.length},  " +
        s"$prepareTime,$fetchTime,${fetched.length},${fetched.map(x => BitUtils.sizeOfSet(x._1)).mkString(":")},   " +
        s"$cm3SolveTime,$cm5NoFixSolveTime,$cm5WithFixSolveTime,$cm5WithFixZeroesSolveTime,$vIPFSolveTime,$eIPFSolveTime,$oneDSolveTime,$avgSolveTime,    " +
        s"$cm3Error,$cm5NoFixError,$cm5WithFixError,$cm5WithFixZeroesError,$vIPFError,$eIPFError,$oneDError,$avgError,  " +
        s"$cm3Entropy,$cm5NoFixEntropy,$cm5WithFixEntropy,$vIPFEntropy,$eIPFEntropy,$oneDEntropy,$avgEntropy,  " +
        s"${vIPFSolver.numIterations},${eIPFSolver.numIterations},  " +
        s"${cliques.size},${cliques.map(_.numVariables).mkString(":")},${cliques.map(_.clusters.size).mkString(":")},    " +
        s"$dof,${muCounts.mkString(":")}"
      fileout.println(resultRow)
    }
  }
  /**
   * Prepare and fetch taken from the moment solver.
   * @param dc DataCube object.
   * @param q Sequence of queried dimensions.
   * @return The fetched cuboids and the maximum dimensionality.
   */
  def prepareFetch(dc: DataCube, q: IndexedSeq[Int]) = {
    val (l, pm) = Profiler("Prepare") {
      dc.index.prepareBatch(q) -> SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
    }

    //Add any missing 1-D marginals if any, for fair comparison for IPF as other solvers have separate access to them anyway
    val unionOfBits = l.map { _.queryIntersection }.reduce(_ | _)
    val total = pm.head._2
    val missing1Dmarginals = pm.tail.flatMap{ case (h, m) =>
      if ((unionOfBits & h) == 0)
        Some(h -> Array(total - m, m))
      else None
    }

    val maxDimFetch = l.last.cuboidCost
    val fetched = Profiler(s"Fetch") { // Same as moment for now
      missing1Dmarginals ++ l.map { pmd => (pmd.queryIntersection, dc.fetch2[Double](List(pmd)))  }
    }
    //println(s"\t\t\tNumber of cubes fetched: ${fetched.length}, Cube sizes (counts): " + s"${
    //  fetched.map { case (bits, _) => BitUtils.sizeOfSet(bits) }
    //    .groupBy(identity).mapValues(_.size).toList.sorted
    //    .map { case (cubeSizes, count) => s"$cubeSizes ($count)" }.mkString(", ")
    //}")
    (fetched, pm, maxDimFetch)
  }
}
