package experiments.thesis.solver

import backend.CBackend
import core.solver.SolverTools.{mk_all_non_neg, preparePrimaryMomentsForQuery}
import core.solver.iterativeProportionalFittingSolver.{MSTVanillaIPFSolver, NewVanillaIPFSolver}
import core.solver.lpp.{Interval, SliceSparseSolver}
import core.solver.moment.CoMoment5SolverDouble
import core.solver.{Rational, SolverTools}
import core.{DataCube, MaterializedQueryResult}
import experiments.ExperimentRunner
import frontend.generators.{CubeGenerator, NYC, SSB}
import frontend.schema.encoders.{ColEncoder, LazyMemCol, StaticDateCol}
import util.{ManualStatsGatherer, Profiler}

class AllSolverOnlineExperiment(ename2: String)(implicit timestampedFolder: String, numIters: Int) extends SolverExperiment(s"all-solvers-online", ename2) {
  val header = "CubeName,RunID,Query,QSize," +
    "SolverName,StatCounter,StatTime,Error"
  fileout.println(header)
  def runNaive(dc: DataCube, query: IndexedSeq[Int], common: String) = {
    val stg = new ManualStatsGatherer[Double]()
    stg.task = () => 0.0
    stg.start()
    val prepared = dc.index.prepareNaive(query)
    val result = dc.fetch(prepared).map(p => p.sm)
    stg.record()
    stg.finish()
    stg.stats.foreach { case (time, count, stat) =>
      val row = s"$common,Naive,$count,$time,$stat"
      fileout.println(row)
    }
    result
  }

  def runLP(dc: DataCube, query: IndexedSeq[Int], trueResult: Array[Double], common: String) {
    if (query.size <= 10) {
      import core.solver.RationalTools._
      val b1 = mk_all_non_neg[Rational](1 << query.size)
      val solver = new SliceSparseSolver[Rational](query.length, b1, Nil, Nil)
      val stg = new ManualStatsGatherer[Vector[Interval[Rational]]]()
      stg.task = () => solver.bounds.toVector
      stg.start()
      var l = dc.index.prepareOnline(query, 2)
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
      stg.stats.foreach { case (time, count, stat) =>
        val error = SolverTools.intervalPrecision(trueResult, stat)
        val row = s"$common,Linear Programming,$count,$time,$error"
        fileout.println(row)
      }
    }
  }

  def runMoment(dc: DataCube, query: IndexedSeq[Int], trueResult: Array[Double], common: String) = {

    val pm = preparePrimaryMomentsForQuery[Double](query, dc.primaryMoments)
    val solver = new CoMoment5SolverDouble(query.length, false, null, pm)
    val stg = new ManualStatsGatherer[Array[Double]]()
    stg.task = () => solver.solution.clone()
    stg.start()
    val l = dc.index.prepareOnline(query, 2)
    val iter = l.toIterator
    while (iter.hasNext) {
      val current = iter.next()
      val fetched = dc.fetch2[Double](List(current))
      solver.add(current.queryIntersection, fetched)
      solver.fillMissing()
      solver.solve(true)
      stg.record()
    }
    stg.finish()
    stg.stats.foreach { case (time, count, stat) =>
      val error = SolverTools.error(trueResult, stat)
      fileout.println(s"$common,Moment,$count,$time,$error")
    }
  }


  def runIPF(dc: DataCube, query: IndexedSeq[Int], trueResult: Array[Double], common: String) = {
    val pm = preparePrimaryMomentsForQuery[Double](query, dc.primaryMoments)
    val solver = new NewVanillaIPFSolver(query.length)
    val stg = new ManualStatsGatherer[Array[Double]]()
    stg.task = () => solver.solution.clone()
    stg.start()
    val l = dc.index.prepareOnline(query, 2)
    solver.initializeWithProductDistribution(pm)
    stg.record()
    val iter = l.toIterator
    while (iter.hasNext) {
      val current = iter.next()
      val fetched = dc.fetch2[Double](List(current))
      solver.add(current.queryIntersection, fetched)
      solver.solve()
      stg.record()
    }
    stg.finish()
    stg.stats.foreach { case (time, count, stat) =>
      val error = SolverTools.error(trueResult, stat)
      fileout.println(s"$common,IPF,$count,$time,$error")
    }
  }

  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Seq()): Unit = {
    val query = qu.sorted
    val common = s"$dcname,$runID,${qu.mkString(";")},${qu.size}"
    val naiveRes = runNaive(dc, query, common)
    val correctRes = if(trueResult != null) trueResult else naiveRes
    //runLP(dc, query, correctRes, common)
    runMoment(dc, query, correctRes, common)
    runIPF(dc, query, correctRes, common)
    runID += 1
  }
}

object AllSolverOnlineExperiment extends ExperimentRunner {

  def expt(cg: CubeGenerator, isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val (minD, maxD) = cg match {
      case n: NYC => (18, 40)
      case s: SSB => (14, 30)
    }
    val logN = 15
    val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
    dc.loadPrimaryMoments(cg.baseName)
    val ename = s"${cg.inputname}-$isSMS"
    val expt = new AllSolverOnlineExperiment(ename)
    val mqr = new MaterializedQueryResult(cg, isSMS)
    val qs = 12

    val queries = mqr.loadQueries(qs).take(numIters)
    queries.zipWithIndex.foreach { case (q, qidx) =>
      val trueResult = mqr.loadQueryResult(qs, qidx)
      expt.run(dc, dc.cubeName, q, trueResult)
    }

    be.reset
  }

  def manual(cg: CubeGenerator, isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val (minD, maxD) = cg match {
      case n: NYC => (18, 40)
      case s: SSB => (14, 30)
    }
    val logN = 15
    val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
    dc.loadPrimaryMoments(cg.baseName)
    val ename = s"${cg.inputname}-$isSMS-manual"
    val expt = new AllSolverOnlineExperiment(ename)
    val sch = cg.schemaInstance
    val encMap = sch.columnVector.map(c => c.name -> c.encoder).toMap[String, ColEncoder[_]]
    val (query, qname) = cg match {
      case n: NYC =>
        val year = encMap("Issue Date").asInstanceOf[StaticDateCol].yearCol.bits
        val month = encMap("Issue Date").asInstanceOf[StaticDateCol].monthCol.bits
        Vector(year, month).reduce(_ ++ _) -> "issue_date_year;issue_date_month"
      case s: SSB =>
        val date = encMap("order_date").asInstanceOf[StaticDateCol]
        val year = date.yearCol.bits
        val brand = encMap("brand").asInstanceOf[LazyMemCol].bits
        Vector(year, brand).reduce(_ ++ _) -> "d_year;p_brand1"
    }
    val qs = query.size
    (0 until numIters).foreach{i =>
      expt.run(dc, dc.cubeName, query, null, qname = qname + s"($qs-D)")
    }
  }


  def main(args: Array[String]) {
    implicit val be = CBackend.default
    val nyc = new NYC()
    val ssb = new SSB(100)

    def func(param: String)(timestamp: String, numIters: Int) = {
      implicit val ni = numIters
      implicit val ts = timestamp
      param match {

        case "nyc-prefix" => expt(nyc, true)
        case "nyc-random" => expt(nyc, false)

        case "ssb-prefix" => expt(ssb, true)
        case "ssb-random" => expt(ssb, false)

        case s => throw new IllegalArgumentException(s)
      }
    }
    run_expt(func)(args)
  }
}


