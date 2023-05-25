package experiments.thesis.solver

import backend.CBackend
import core.solver.Rational
import core.solver.SolverTools.{intervalPrecision, mk_all_non_neg}
import core.solver.lpp.{Interval, SliceSparseSolver}
import core.{DataCube, MaterializedQueryResult}
import experiments.ExperimentRunner
import frontend.generators.{CubeGenerator, NYC, SSB}
import util.{ManualStatsGatherer, Profiler}

class LPSolverOnlineExperiment[T](ename2: String = "", additionalHeaders: String, f: SliceSparseSolver[Rational] => T, statToString: (T, Array[Double]) => String)(implicit timestampedFolder: String, numIters: Int) extends SolverExperiment(s"lp-solver-online", ename2) {

  import core.solver.RationalTools._

  val header = "CubeName,RunID,Query,QSize,StatCounter,StatTime," + additionalHeaders
  fileout.println(header)
  def prepareOnline(dc: DataCube, query: IndexedSeq[Int]) = {
    val l = Profiler("PrepareOnline") {
      dc.index.prepareOnline(query, 2)
    }
    l
  }
  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Seq()): Unit = {
    val query = qu.sorted
    Profiler.resetAll()

    val b1 = mk_all_non_neg[Rational](1 << query.size)
    val solver = new SliceSparseSolver[Rational](query.length, b1, Nil, Nil)
    val stg = new ManualStatsGatherer[T]()
    stg.task = () => f(solver)
    stg.start()
    var l = prepareOnline(dc, query)
    while (!(l.isEmpty) && solver.df > 0) {
      val bits = l.head.queryIntersection
      if (solver.shouldFetch(bits)) {
        val fetched = dc.fetch2[Rational](List(l.head))
        solver.add2(List(bits), fetched)
        solver.gauss(solver.det_vars)
        solver.compute_bounds
        stg.record()
      }
      l = l.tail
    }
    stg.finish()
    if (output) {
      stg.stats.foreach { case (time, count, stat) =>
        val str = statToString(stat, trueResult)
        fileout.println(s"$dcname,$runID,${qu.mkString(";")},${query.size},$count,$time,$str")
      }
    }
    runID += 1
  }

}

object LPSolverOnlineExperiment extends ExperimentRunner {

  import core.solver.RationalTools._

  val stats1 = {
    def stats(s: SliceSparseSolver[Rational]) = (s.df, s.bounds.toVector)
    def statsToString(stat: (Int, Vector[Interval[Rational]]), trueResult: Array[Double]) = {
      val df = stat._1
      val precision = intervalPrecision(trueResult, stat._2)
      s"$df,$precision"
    }
    val header = "DOF,IntervalSpan"
    (header, stats(_), statsToString(_, _))
  }

  def qsize(cg: CubeGenerator, isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val (minD, maxD) = cg match {
      case n: NYC => (18, 40)
      case s: SSB => (14, 30)
    }
    val logN = 15
    val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
    val ename = s"${cg.inputname}-$isSMS-qsize"
    val (header, statf, strf) = stats1
    val expt = new LPSolverOnlineExperiment(ename, header, statf, strf)
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

  def minD(cg: CubeGenerator, isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val qsize = 10
    val (minDLast, maxD) = cg match {
      case n: NYC => (18, 40)
      case s: SSB => (14, 30)
    }
    val logN = 15

    val ename = s"${cg.inputname}-$isSMS-minD"
    val mqr = new MaterializedQueryResult(cg, isSMS)
    val queries = mqr.loadQueries(qsize).take(numIters)
    val (header, statf, strf) = stats1
    val expt = new LPSolverOnlineExperiment(ename, header, statf, strf)
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
    val (header, statf, strf) = stats1
    val expt = new LPSolverOnlineExperiment(ename, header, statf, strf)
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
    throw new IllegalArgumentException("Disabled Experiment")
    run_expt(func)(args)
  }
}