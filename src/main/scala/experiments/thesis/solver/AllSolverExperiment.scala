package experiments.thesis.solver

import backend.CBackend
import core.{DataCube, MaterializedQueryResult}
import core.solver.iterativeProportionalFittingSolver.MSTVanillaIPFSolver
import core.solver.lpp.SliceSparseSolver
import core.solver.moment.CoMoment5SolverDouble
import core.solver.{Rational, SolverTools}
import experiments.ExperimentRunner
import frontend.generators.{CubeGenerator, NYC, SSB}
import util.Profiler

class AllSolverExperiment(ename2: String)(implicit timestampedFolder: String, numIters: Int) extends SolverExperiment(s"all-solvers", ename2) {
  val header = "CubeName,RunID,Query,QSize," +
    "PrepareNaive,FetchNaive,TotalNaive," +
    "PrepareLP,FetchLP,SolveLP,TotalLP,ErrorLP," +
    "PrepareMoment,FetchMoment,SolveMoment,TotalMoment,ErrorMoment," +
    "PrepareIPF,FetchIPF,SolveIPF,TotalIPF,ErrorIPF"
  fileout.println(header)
  def runNaive(dc: DataCube, query: IndexedSeq[Int]) = {
    Profiler.resetAll()
    val prepared = Profiler("PrepareNaive") { dc.index.prepareNaive(query) }
    val result = Profiler("FetchNaive") { dc.fetch(prepared).map(p => p.sm) }

    val prepareTime = Profiler.getDurationMicro("PrepareNaive")
    val fetchTime = Profiler.getDurationMicro("FetchNaive")
    val totalTime = prepareTime + fetchTime
    s"$prepareTime,$fetchTime,$totalTime"
  }

  def runLP(dc: DataCube, query: IndexedSeq[Int], trueResult: Array[Double]) = {
    Profiler.resetAll()
    if(query.size <= 10) {
      import core.solver.RationalTools._
      val prepared = Profiler("PrepareLP") {
        dc.index.prepareBatch(query) //fetch most dominating cuboids other than full
      }
      val fetched = Profiler("FetchLP") {
        dc.fetch2[Rational](prepared)
      }
      val result = Profiler("SolveLP") {
        val allVars = 0 until (1 << query.size)
        val s1 = Profiler("Init SliceSparseSolver") {
          val b1 = SolverTools.mk_all_non_neg[Rational](1 << query.size)
          new SliceSparseSolver(query.length, b1, prepared.map(_.queryIntersection), fetched)
        }
        Profiler("ComputeBounds SliceSparse") {
          s1.compute_bounds
          s1.propagate_bounds(allVars)
        }
        s1.bounds
      }
      val prepareTime = Profiler.getDurationMicro("PrepareLP")
      val fetchTime = Profiler.getDurationMicro("FetchLP")
      val solveTime = Profiler.getDurationMicro("SolveLP")
      val totalTime = prepareTime + fetchTime + solveTime
      val error = SolverTools.intervalPrecision(trueResult, result)
      s"$prepareTime,$fetchTime,$solveTime,$totalTime,$error"
    } else {
      s"prepare,fetch,solve,total,error"
    }
  }
  def runMoment(dc: DataCube, query: IndexedSeq[Int], trueResult: Array[Double]) = {
    Profiler.resetAll()
    val (prepared, pm) = Profiler("PrepareMoment") {
      dc.index.prepareBatch(query) -> SolverTools.preparePrimaryMomentsForQuery[Double](query, dc.primaryMoments)
    }
    val fetched = Profiler("FetchMoment") {
      prepared.map { pm => pm.queryIntersection -> dc.fetch2[Double](List(pm)) }
    }
    val result = Profiler("SolveMoment") {
      val s = new CoMoment5SolverDouble(query.size, true, null, pm)
      fetched.foreach { case (cols, data) => s.add(cols, data) }
      s.fillMissing()
      s.solve()
    }
    val prepareTime = Profiler.getDurationMicro("PrepareMoment")
    val fetchTime = Profiler.getDurationMicro("FetchMoment")
    val solveTime = Profiler.getDurationMicro("SolveMoment")
    val totalTime = prepareTime + fetchTime + solveTime
    val error = SolverTools.error(result, trueResult)
    s"$prepareTime,$fetchTime,$solveTime,$totalTime,$error"
  }


  def runIPF(dc: DataCube, query: IndexedSeq[Int], trueResult: Array[Double]) = {
    Profiler.resetAll()
    val prepared = Profiler("PrepareIPF") {
      dc.index.prepareBatch(query)
    }
    val fetched = Profiler("FetchIPF") {
      prepared.map { pm => pm.queryIntersection -> dc.fetch2[Double](List(pm)) }
    }
    val result = Profiler("SolveIPF") {
      val s = new MSTVanillaIPFSolver(query.size)
      fetched.foreach { case (cols, data) => s.add(cols, data) }
      s.solve()
    }
    val prepareTime = Profiler.getDurationMicro("PrepareIPF")
    val fetchTime = Profiler.getDurationMicro("FetchIPF")
    val solveTime = Profiler.getDurationMicro("SolveIPF")
    val totalTime = prepareTime + fetchTime + solveTime
    val error = SolverTools.error(result, trueResult)
    s"$prepareTime,$fetchTime,$solveTime,$totalTime,$error"
  }

  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Seq()): Unit = {
    val query = qu.sorted
    val common = s"$dcname,$runID,${qu.mkString(";")},${qu.size}"
    val naive = runNaive(dc, query)
    val lp = runLP(dc, query, trueResult)
    val moment = runMoment(dc, query, trueResult)
    val ipf = runIPF(dc, query, trueResult)
    val row = s"$common,$naive,$lp,$moment,$ipf"
    fileout.println(row)
    runID += 1
  }
}

object AllSolverExperiment extends ExperimentRunner {

  def qsize(cg: CubeGenerator, isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val (minD, maxD) = cg match {
      case n: NYC => (18, 40)
      case s: SSB => (14, 30)
    }
    val logN = 15
    val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
    dc.loadPrimaryMoments(cg.baseName)
    val ename = s"${cg.inputname}-$isSMS-qsize"
    val expt = new AllSolverExperiment(ename)
    val mqr = new MaterializedQueryResult(cg, isSMS)
    Vector(2, 4, 6, 8, 10, 12, 14, 18).reverse.map { qs =>
      val queries = mqr.loadQueries(qs).take(numIters)
      queries.zipWithIndex.foreach { case (q, qidx) =>
        val trueResult = mqr.loadQueryResult(qs, qidx)
        expt.run(dc, dc.cubeName, q, trueResult)
      }
    }
    be.reset
  }

  //def minD(cg: CubeGenerator, isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
  //  val qsize = if (isSMS) 6 else 4
  //  val (minDLast, maxD) = cg match {
  //    case n: NYC => (18, 40)
  //    case s: SSB => (14, 30)
  //  }
  //  val logN = 15
  //
  //  val ename = s"${cg.inputname}-$isSMS-minD"
  //  //we allow queries to repeat.
  //  val mqr = new MaterializedQueryResult(cg, isSMS)
  //  val queries = mqr.loadQueries(qsize).take(numIters)
  //  val expt = new AllSolverExperiment(ename)
  //  (6 to minDLast).by(4).reverse.map { minD =>
  //    val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
  //    dc.loadPrimaryMoments(cg.baseName)
  //    queries.zipWithIndex.foreach { case (q, qidx) =>
  //        val trueResult = mqr.loadQueryResult(qs, qidx)
  //        expt.run(dc, dc.cubeName, q, trueResult)
  //      }
  //    be.reset
  //  }
  //}

  //def logN(cg: CubeGenerator, isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
  //  val qsize = if (isSMS) 6 else 4
  //  val (minD, maxD) = cg match {
  //    case n: NYC => (18, 40)
  //    case s: SSB => (14, 30)
  //  }
  //
  //  val ename = s"${cg.inputname}-$isSMS-logN"
  //  //we allow queries to repeat.
  //  val mqr = new MaterializedQueryResult(cg, isSMS)
  //  val queries = mqr.loadQueries(qsize).take(numIters)
  //  val expt = new AllSolverExperiment(ename)
  //  (6 to 15).by(3).reverse.map { logN =>
  //    val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
  //    dc.loadPrimaryMoments(cg.baseName)
  //    queries.zipWithIndex.foreach { case (q, qidx) =>
  //        val trueResult = mqr.loadQueryResult(qs, qidx)
  //        expt.run(dc, dc.cubeName, q, trueResult)
  //      }
  //    be.reset
  //  }
  //}

  def main(args: Array[String]) {
    implicit val be = CBackend.default
    val nyc = new NYC()
    val ssb = new SSB(100)

    def func(param: String)(timestamp: String, numIters: Int) = {
      implicit val ni = numIters
      implicit val ts = timestamp
      param match {

        case "qsize-nyc-prefix" => qsize(nyc, true)
        case "qsize-nyc-random" => qsize(nyc, false)

        case "qsize-ssb-prefix" => qsize(ssb, true)
        case "qsize-ssb-random" => qsize(ssb, false)

        //case "logn-nyc-prefix" => logN(nyc, true)
        //case "logn-nyc-random" => logN(nyc, false)
        //
        //case "logn-ssb-prefix" => logN(ssb, true)
        //case "logn-ssb-random" => logN(ssb, false)
        //
        //case "mind-nyc-prefix" => minD(nyc, true)
        //case "mind-nyc-random" => minD(nyc, false)
        //
        //case "mind-ssb-prefix" => minD(ssb, true)
        //case "mind-ssb-random" => minD(ssb, false)

        case s => throw new IllegalArgumentException(s)
      }
    }
    run_expt(func)(args)
  }
}


