package experiments.thesis.solver

import backend.CBackend
import core.solver.SolverTools._
import core.solver.moment.Strategy.CoMoment
import core.solver.moment.{CoMoment3Solver, CoMoment5SolverDouble, Moment1Transformer, MomentSolverAll}
import core.{DataCube, MaterializedQueryResult}
import experiments.ExperimentRunner
import frontend.generators.{CubeGenerator, NYC, SSB}
import planning.NewProjectionMetaData
import util.Profiler

class CoMomentSolverBatchExperiment(ename2: String = "", handleNegative: Boolean, runOldSolvers: Boolean)(implicit timestampedFolder: String, numIters: Int) extends SolverExperiment(s"comoment-batch", ename2) {
  val header = "CubeName,RunID,Query,QSize," +
    "PrepareTime(us),FetchTime(us)," +
    "DOF," +
    "SolveTimeCM1(us),CM1ErrL1," +
    "SolveTimeCM3(us),CM3ErrL1," +
    "SolveTimeCM5(us),CM5ErrL1"
  fileout.println(header)
  def prepareAndFetch(dc: DataCube, query: IndexedSeq[Int]) = {
    val (l, pm) = Profiler(s"Prepare") { // Not doing prepare for primary moments
      dc.index.prepareBatch(query) -> preparePrimaryMomentsForQuery[Double](query, dc.primaryMoments)
    }
    val fetched = Profiler(s"Fetch") { // Same as moment for now
      l.map { pm => (pm -> dc.fetch2[Double](List(pm))) }
    }
    (pm, fetched)
  }

  def momentSolveCM1(query: IndexedSeq[Int], cuboids: Seq[(NewProjectionMetaData, Array[Double])]) = {
    Profiler(s"MomentSolve CM1") {
      val solver = new MomentSolverAll[Double](query.size, CoMoment)
      cuboids.foreach { case (pm, array) => solver.add(pm.queryIntersection, array) }
      solver.fillMissing()
      solver.fastSolve(handleNegative)
      solver
    }
  }
  def momentSolveCM3(query: IndexedSeq[Int], pm: Seq[(Int, Double)], cuboids: Seq[(NewProjectionMetaData, Array[Double])]) = {
    Profiler(s"MomentSolve CM3") {
      val solver = new CoMoment3Solver[Double](query.size, true, new Moment1Transformer[Double](), pm)
      cuboids.foreach { case (pm, array) => solver.add(pm.queryIntersection, array) }
      solver.fillMissing()
      solver.solve(handleNegative)
      solver
    }
  }
  def momentSolveCM5(query: IndexedSeq[Int], pm: Seq[(Int, Double)], cuboids: Seq[(NewProjectionMetaData, Array[Double])]) = {
    Profiler(s"MomentSolve CM5") {
      val solver = new CoMoment5SolverDouble(query.size, true, null, pm)
      cuboids.foreach { case (pm, array) => solver.add(pm.queryIntersection, array) }
      solver.fillMissing()
      solver.solve(handleNegative)
      solver
    }
  }
  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Seq()): Unit = {
    val query = qu.sorted
    Profiler.resetAll()
    val (pm, fetched) = prepareAndFetch(dc, query)
    val prepareTime = Profiler.getDurationMicro("Prepare")
    val fetchTime = Profiler.getDurationMicro("Fetch")

    val cm5 = momentSolveCM5(query, pm, fetched)
    val dof = cm5.dof
    val cm5SolveTime = Profiler.getDurationMicro("MomentSolve CM5")
    val cm5ErrL1 = error(trueResult, cm5.solution)
    val cm5Row = s"$cm5SolveTime,$cm5ErrL1"
    val cm1row = if (runOldSolvers) {
      val cm1 = momentSolveCM1(query, fetched)
      val cm1SolveTime = Profiler.getDurationMicro("MomentSolve CM1")
      val cm1ErrL1 = error(trueResult, cm1.solution)
      s"$cm1SolveTime,$cm1ErrL1"
    } else {
      ","
    }

    val cm3row = if (runOldSolvers) {
      val cm3 = momentSolveCM3(query, pm, fetched)
      val cm3SolveTime = Profiler.getDurationMicro("MomentSolve CM3")
      val cm3ErrL1 = error(trueResult, cm3.solution)
      s"$cm3SolveTime,$cm3ErrL1"
    } else {
      ","
    }

    val row = s"$dcname,$runID,${qu.mkString(";")},${qu.size}," +
      s"$prepareTime,$fetchTime," +
      s"$dof," +
      s"$cm1row," +
      s"$cm3row," +
      s"$cm5Row"
    fileout.println(row)
    runID += 1
  }
}

object CoMomentSolverBatchExperiment extends ExperimentRunner{

  def qsize(cg: CubeGenerator, isSMS: Boolean, handleNegative: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val (minD, maxD) = cg match {
      case n: NYC => (18, 40)
      case s: SSB => (14, 30)
    }
    val logN = 15
    val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
    dc.loadPrimaryMoments(cg.baseName)
    val ename = s"${cg.inputname}-$isSMS-qsize-$handleNegative"
    val expt = new CoMomentSolverBatchExperiment(ename, handleNegative, runOldSolvers = true)
    val mqr = new MaterializedQueryResult(cg, isSMS)
    Vector(6, 9, 12, 15).reverse.map { qs =>
      val queries = mqr.loadQueries(qs).take(numIters)
      queries.zipWithIndex.foreach { case (q, qidx) =>
        val trueRes = mqr.loadQueryResult(qs, qidx)
        expt.run(dc, dc.cubeName, q, trueRes)
      }
    }
    be.reset
  }

  def minD(cg: CubeGenerator, isSMS: Boolean, handleNegative: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val qsize = 10
    val (minDLast, maxD) = cg match {
      case n: NYC => (18, 40)
      case s: SSB => (14, 30)
    }
    val logN = 15

    val ename = s"${cg.inputname}-$isSMS-minD-$handleNegative"
    val mqr = new MaterializedQueryResult(cg, isSMS)
    val queries = mqr.loadQueries(qsize).take(numIters)
    val expt = new CoMomentSolverBatchExperiment(ename, handleNegative, false)
    (6 to minDLast).by(4).reverse.map { minD =>
      val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
      dc.loadPrimaryMoments(cg.baseName)
      queries.zipWithIndex.foreach { case (q, qidx) =>
        val trueResult = mqr.loadQueryResult(qsize, qidx)
        expt.run(dc, dc.cubeName, q, trueResult)
      }
      be.reset
    }
  }

  def logN(cg: CubeGenerator, isSMS: Boolean, handleNegative: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val qsize = 10
    val (minD, maxD) = cg match {
      case n: NYC => (18, 40)
      case s: SSB => (14, 30)
    }

    val ename = s"${cg.inputname}-$isSMS-logN-$handleNegative"
    val mqr = new MaterializedQueryResult(cg, isSMS)
    val queries = mqr.loadQueries(qsize).take(numIters)
    val expt = new CoMomentSolverBatchExperiment(ename, handleNegative, false)
    (6 to 15).by(3).reverse.map { logN =>
      val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
      dc.loadPrimaryMoments(cg.baseName)
      queries.zipWithIndex.foreach { case (q, qidx) =>
        val trueResult = mqr.loadQueryResult(qsize, qidx)
        expt.run(dc, dc.cubeName, q, trueResult)
      }
      be.reset
    }
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

        case "logn-nyc-prefix-true" => logN(nyc, true, true)
        case "logn-nyc-prefix-false" => logN(nyc, true, false)
        case "logn-nyc-random-true" => logN(nyc, false, true)
        case "logn-nyc-random-false" => logN(nyc, false, false)

        case "logn-ssb-prefix-true" => logN(ssb, true, true)
        case "logn-ssb-prefix-false" => logN(ssb, true, false)
        case "logn-ssb-random-true" => logN(ssb, false, true)
        case "logn-ssb-random-false" => logN(ssb, false, false)

        case "mind-nyc-prefix-true" => minD(nyc, true, true)
        case "mind-nyc-prefix-false" => minD(nyc, true, false)
        case "mind-nyc-random-true" => minD(nyc, false, true)
        case "mind-nyc-random-false" => minD(nyc, false, false)

        case "mind-ssb-prefix-true" => minD(ssb, true, true)
        case "mind-ssb-prefix-false" => minD(ssb, true, false)
        case "mind-ssb-random-true" => minD(ssb, false, true)
        case "mind-ssb-random-false" => minD(ssb, false, false)

        case s => throw new IllegalArgumentException(s)
      }
    }
    run_expt(func)(args)
  }
}
