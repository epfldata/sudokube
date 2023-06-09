package experiments.thesis.solver

import backend.CBackend
import core.solver.SolverTools._
import core.solver.lpp.SliceSparseSolver
import core.solver.{Rational, SolverTools}
import core.{DataCube, MaterializedQueryResult}
import experiments.ExperimentRunner
import frontend.generators._
import planning.NewProjectionMetaData
import util.{BitUtils, Profiler}



class LPSolverBatchExperiment(ename2: String = "")(implicit timestampedFolder: String, numIters: Int) extends SolverExperiment(s"lp-solver-batch", ename2) {
  import core.solver.RationalTools._
  val header = "CubeName,RunID,Query,QSize," +
    "PrepareTime(us),FetchTime(us)," +
    "SolveTime(us)," +
    "NumMoments,DOF,NumSolved,NumZeroesInQResult," +
    "IntervalSpan,MidpointErrorL1"
  fileout.println(header)
  def prepareAndFetchBatch(dc: DataCube, query: IndexedSeq[Int]) = {
    val l = Profiler("Prepare") {
      dc.index.prepareBatch(query) //fetch most dominating cuboids other than full
    }
    val fetched = Profiler("Fetch") {
      dc.fetch2[Rational](l)
    }
    (l, fetched)
  }
  def runLPBatch(query: IndexedSeq[Int], prepared: Seq[NewProjectionMetaData], fetched: Array[Rational]) = {
    Profiler("LP Solve") {
      val allVars = 0 until (1 << query.size)
      val s1 = Profiler("Init SliceSparseSolver") {
        val b1 = SolverTools.mk_all_non_neg[Rational](1 << query.size)
        new SliceSparseSolver(query.length, b1, prepared.map(_.queryIntersection), fetched)
      }
      Profiler("ComputeBounds SliceSparse") {
        s1.compute_bounds
        s1.propagate_bounds(allVars)
      }
      s1
    }
  }
  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Seq()): Unit = {
    val query = qu.sorted
    Profiler.resetAll()
    val (prepared, fetched) = prepareAndFetchBatch(dc, query)
    val solver = runLPBatch(query, prepared, fetched)

    val prepareTime = Profiler.getDurationMicro("Prepare")
    val fetchTime = Profiler.getDurationMicro("Fetch")
    val solverTime = Profiler.getDurationMicro("LP Solve")
    val knownSet = collection.mutable.BitSet()
    prepared.foreach { pm =>
      val cols = pm.queryIntersection
      val n0 = (1 << pm.queryIntersectionSize)
      (0 until n0).map { i0 =>
        val i = BitUtils.unprojectIntWithInt(i0, cols)
        knownSet += i
      }
    }
    val bounds = solver.bounds.toArray
    val midpoint = bounds.map(i => (i.lb.get + i.ub.get) / 2)

    val precision = intervalPrecision(trueResult, bounds)
    val midpointErrorL1 = error(trueResult, midpoint)
    val numMoments = knownSet.size
    val numZeroesInQueryResult = trueResult.count(_ == 0.0)
    val dof = solver.df
    val numSolved = solver.solved_vars.size
    val row = s"$dcname,$runID,${qu.mkString(";")},${qu.size}," +
      s"$prepareTime,$fetchTime," +
      s"$solverTime," +
      s"$numMoments,$dof,$numSolved,$numZeroesInQueryResult," +
      s"$precision,$midpointErrorL1"
    fileout.println(row)
    runID += 1
  }
}

object LPSolverBatchExperiment extends ExperimentRunner {
  def qsize(cg: CubeGenerator, isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val (minD, maxD) = cg match {
      case n: NYC => (18, 40)
      case s: SSB => (14, 30)
    }
    val logN = 15
    val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
    val ename = s"${cg.inputname}-$isSMS-qsize"
    val expt = new LPSolverBatchExperiment(ename)
    val mqr = new MaterializedQueryResult(cg, isSMS)
    Vector(6, 8, 10).reverse.map { qs =>
      val queries = mqr.loadQueries(qs).take(numIters)
      queries.zipWithIndex.foreach { case (q, qidx) =>
        val trueRes = mqr.loadQueryResult(qs, qidx)
        expt.run(dc, dc.cubeName, q, trueRes)
      }
    }
    be.reset
  }

  def  minD(cg: CubeGenerator, isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val qsize = 10
    val (minDLast, maxD) = cg match {
      case n: NYC => (18, 40)
      case s: SSB => (14, 30)
    }
    val logN = 15

    val ename = s"${cg.inputname}-$isSMS-minD"
    val mqr = new MaterializedQueryResult(cg, isSMS)
    val queries = mqr.loadQueries(qsize).take(numIters)
    val expt = new LPSolverBatchExperiment(ename)
    (6 to minDLast).by(4).reverse.map { minD =>
      val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
      queries.zipWithIndex.foreach { case (q, qidx) =>
        val trueResult = mqr.loadQueryResult(qsize, qidx)
        expt.run(dc, dc.cubeName, q, trueResult)
      }
      be.reset
    }
  }

  def logN(cg: CubeGenerator, isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val qsize = 10
    val (minD, maxD) = cg match {
      case n: NYC => (18, 40)
      case s: SSB => (14, 30)
    }

    val ename = s"${cg.inputname}-$isSMS-logN"
    val mqr = new MaterializedQueryResult(cg, isSMS)
    val queries = mqr.loadQueries(qsize).take(numIters)
    val expt = new LPSolverBatchExperiment(ename)
    (6 to 15).by(3).reverse.map { logN =>
      val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
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

        case "qsize-nyc-prefix" => qsize(nyc, true)
        case "qsize-nyc-random" => qsize(nyc, false)

        case "qsize-ssb-prefix" => qsize(ssb, true)
        case "qsize-ssb-random" => qsize(ssb, false)

        case "logn-nyc-prefix" => logN(nyc, true)
        case "logn-nyc-random" => logN(nyc, false)

        case "logn-ssb-prefix" => logN(ssb, true)
        case "logn-ssb-random" => logN(ssb, false)

        case "mind-nyc-prefix" => minD(nyc, true)
        case "mind-nyc-random" => minD(nyc, false)

        case "mind-ssb-prefix" => minD(ssb, true)
        case "mind-ssb-random" => minD(ssb, false)

        case s => throw new IllegalArgumentException(s)
      }
    }
    run_expt(func)(args)
  }
}
