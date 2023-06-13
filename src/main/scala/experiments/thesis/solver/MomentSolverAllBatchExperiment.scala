package experiments.thesis.solver

import backend.CBackend
import core.solver.SolverTools._
import core.solver.moment.MomentSolverAll
import core.solver.moment.Strategy._
import core.{DataCube, MaterializedQueryResult}
import experiments.ExperimentRunner
import frontend.generators.{StaticCubeGenerator => CubeGenerator, NYC, SSB}
import planning.NewProjectionMetaData
import util.Profiler

class MomentSolverAllBatchExperiment(ename2: String = "", handleNegative: Boolean)(implicit timestampedFolder: String, numIters: Int) extends SolverExperiment(s"moment-solver-all-batch", ename2) {
  val strategies = Vector(Zero, HalfPowerD, Avg, Avg2, FrechetUpper, FrechetMid, CoMoment)
  val header = "CubeName,RunID,Query,QSize," +
    "PrepareTime(us),FetchTime(us)," +
    "DOF," +
    strategies.map { st => s"${st}SolveTime(us),${st}L1Error" }.mkString(",")
  fileout.println(header)
  def momentSolve(query: IndexedSeq[Int], strategy: Strategy, cuboids: Seq[(NewProjectionMetaData, Array[Double])]) = {
    Profiler(s"MomentSolve $strategy") {
      val solver = new MomentSolverAll[Double](query.size, strategy)
      cuboids.foreach { case (pm, array) => solver.add(pm.queryIntersection, array) }
      solver.fillMissing()
      solver.fastSolve(handleNegative)
      solver
    }
  }

  def prepareAndFetch(dc: DataCube, query: IndexedSeq[Int]) = {
    val l = Profiler(s"Prepare") { // Not doing prepare for primary moments
      dc.index.prepareBatch(query)
    }
    val fetched = Profiler(s"Fetch") { // Same as moment for now
      l.map { pm => (pm -> dc.fetch2[Double](List(pm))) }
    }
    fetched
  }
  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Seq()): Unit = {
    val query = qu.sorted

    Profiler.resetAll()
    val fetched = prepareAndFetch(dc, query)
    val prepareTime = Profiler.getDurationMicro("Prepare")
    val fetchTime = Profiler.getDurationMicro("Fetch")

    val results = strategies.map { strategy =>
      val solver = momentSolve(query, strategy, fetched)
      val solveTime = Profiler.getDurationMicro(s"MomentSolve $strategy")
      val errorL1 = error(trueResult, solver.solution)
      solver.dof -> s"$solveTime,$errorL1"
    }

    val dof = results.head._1
    val solveTimeErrors = results.map(_._2).mkString(",")
    val row = s"$dcname,$runID,${qu.mkString(";")},${qu.size}," +
      s"$prepareTime,$fetchTime," +
      s"$dof," +
      solveTimeErrors
    fileout.println(row)
    runID += 1
  }
}


object MomentSolverAllBatchExperiment extends ExperimentRunner {

  def qsize(cg: CubeGenerator, isSMS: Boolean, handleNegative: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val (minD, maxD) = cg match {
      case n: NYC => (18, 40)
      case s: SSB => (14, 30)
    }
    val logN = 15
    val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
    val ename = s"${cg.inputname}-$isSMS-qsize-$handleNegative"
    val expt = new MomentSolverAllBatchExperiment(ename, handleNegative)
    val mqr = new MaterializedQueryResult(cg, isSMS)
    Vector(6, 8, 10, 12).reverse.map { qs =>
      val queries = mqr.loadQueries(qs).take(numIters)
      queries.zipWithIndex.foreach { case (q, qidx) =>
        val trueRes = mqr.loadQueryResult(qs, qidx)
        expt.run(dc, dc.cubeName, q, trueRes)
      }
    }
    be.reset
  }

  def main(args: Array[String]) = {
    implicit val be = CBackend.default
    val nyc = new NYC()
    val ssb = new SSB(100)

    def func(param: String)(timestamp: String, numIters: Int) = {
      implicit val ni = numIters
      implicit val ts = timestamp
      param match {
        case "qsize-nyc-prefix-true" => qsize(nyc, true, true)
        case "qsize-nyc-prefix-false" => qsize(nyc, true, false)
        case "qsize-nyc-random-true" => qsize(nyc, false, true)
        case "qsize-nyc-random-false" => qsize(nyc, false, false)

        case "qsize-ssb-prefix-true" => qsize(ssb, true, true)
        case "qsize-ssb-prefix-false" => qsize(ssb, true, false)
        case "qsize-ssb-random-true" => qsize(ssb, false, true)
        case "qsize-ssb-random-false" => qsize(ssb, false, false)

        case s => throw new IllegalArgumentException(s)
      }
    }
    run_expt(func)(args)
  }
}