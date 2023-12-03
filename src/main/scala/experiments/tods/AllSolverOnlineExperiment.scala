package experiments.tods

import backend.CBackend
import core.solver.SolverTools.{mk_all_non_neg, preparePrimaryMomentsForQuery}
import core.solver.iterativeProportionalFittingSolver.NewVanillaIPFSolver
import core.solver.lpp.{Interval, SliceSparseSolver}
import core.solver.moment.CoMoment5SolverDouble
import core.solver.{Rational, SolverTools}
import core.{DataCube, MaterializedQueryResult}
import experiments.ExperimentRunner
import frontend.generators.{CubeGenerator, NYC, SSB}
import frontend.schema.encoders.{ColEncoder, LazyMemCol, StaticDateCol, StaticNatCol}
import util.ManualStatsGatherer

class AllSolverOnlineExperiment(ename2: String)(implicit timestampedFolder: String, numIters: Int) extends SolverExperiment(s"all-solvers-online", ename2) {
  val header = "CubeName,RunID,Query,QSize,QName," +
    "SolverName,StatCounter,StatTime,Error"
  fileout.println(header)
  var shouldRunNaive = true
  var shouldRunLP = false
  var shouldRunIPF = true
  var shouldRunMoment = true

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

  def runMoment(dc: DataCube, query: IndexedSeq[Int], trueResult: Array[Double], common: String, errorFix: Boolean) = {

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
    val suffixName = if(errorFix) "with fix" else "no fix"
    stg.finish()
    stg.stats.foreach { case (time, count, stat) =>
      val error = SolverTools.error(trueResult, stat)
      fileout.println(s"$common,Moment ($suffixName),$count,$time,$error")
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
    //    stg.record()
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
    val common = s"$dcname,$runID,${qu.mkString(";")},${qu.size},$qname"
    val naiveRes = if (shouldRunNaive) runNaive(dc, query, common) else null
    val correctRes = if (trueResult != null) trueResult else naiveRes
    if (shouldRunLP) runLP(dc, query, correctRes, common)
    if (shouldRunMoment) {
      runMoment(dc, query, correctRes, common, true)
      runMoment(dc, query, correctRes, common, false)
    }
    if (shouldRunIPF) runIPF(dc, query, correctRes, common)
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

  def manual_queries(cg: CubeGenerator, isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val (minD, maxD) = cg match {
      case n: NYC => (18, 40)
      case s: SSB => (14, 30)
    }
    val logN = 15
    val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
    dc.loadPrimaryMoments(cg.baseName)
    val ename = s"${cg.inputname}-$isSMS-manual-queries"
    val expt = new AllSolverOnlineExperiment(ename)
    expt.shouldRunNaive = false
    expt.shouldRunLP = false
    expt.shouldRunMoment = false
    expt.shouldRunIPF = true
    val sch = cg.schemaInstance
    val encMap = sch.columnVector.map(c => c.name -> c.encoder).toMap[String, ColEncoder[_]]
    val queries = cg match {
      case s: SSB =>
        val date = encMap("order_date").asInstanceOf[StaticDateCol]
        val year = date.yearCol.bits
        val discount = encMap("discount").asInstanceOf[StaticNatCol].bits
        val qty = encMap("quantity").asInstanceOf[StaticNatCol].bits
        val category = encMap("category").asInstanceOf[LazyMemCol].bits
        val brand = encMap("brand").asInstanceOf[LazyMemCol].bits
        val snation = encMap("supp_nation").asInstanceOf[LazyMemCol].bits
        val sregion = encMap("supp_region").asInstanceOf[LazyMemCol].bits
        val scity = encMap("supp_city").asInstanceOf[LazyMemCol].bits
        val cregion = encMap("cust_region").asInstanceOf[LazyMemCol].bits
        val mfgr = encMap("mfgr").asInstanceOf[LazyMemCol].bits
        val tax = encMap("tax").asInstanceOf[StaticNatCol].bits
        val mktSegment = encMap("cust_mkt_segment").asInstanceOf[LazyMemCol].bits
        val ord_prio = encMap("ord_priority").asInstanceOf[LazyMemCol].bits
        val commit_year = encMap("commit_date").asInstanceOf[StaticDateCol].yearCol.bits
        Vector(
          Vector(brand.drop(1), snation, cregion) -> "brand/2;s_nation;cust_region", //Q1
          Vector(scity, mfgr, tax) -> "s_city;mfgr;tax", //Q2
          Vector(year, discount, qty) -> "d_year;lo_discount;lo_quantity", //Q3
          Vector(ord_prio, mktSegment, category, commit_year) -> "ord_priority;mkt_segment;commit_date_year;category" //Q4
        )
      case n: NYC =>
        val year = encMap("Issue Date").asInstanceOf[StaticDateCol].yearCol.bits
        val issuePrecinct = encMap("Issuer Precinct").asInstanceOf[LazyMemCol].bits
        val state = encMap("Registration State").asInstanceOf[LazyMemCol].bits
        val body = encMap("Vehicle Body Type").asInstanceOf[LazyMemCol].bits
        val make = encMap("Vehicle Make").asInstanceOf[LazyMemCol].bits
        val ptype = encMap("Plate Type").asInstanceOf[LazyMemCol].bits
        val agency = encMap("Issuing Agency").asInstanceOf[LazyMemCol].bits
        val squad = encMap("Issuer Squad").asInstanceOf[LazyMemCol].bits
        val precinct = encMap("Violation Precinct").asInstanceOf[LazyMemCol].bits
        val color = encMap("Vehicle Color").asInstanceOf[LazyMemCol].bits
        val lawsect = encMap("Law Section").asInstanceOf[LazyMemCol].bits
        val code = encMap("Violation Code").asInstanceOf[LazyMemCol].bits
        val county = encMap("Violation County").asInstanceOf[LazyMemCol].bits
        Vector(
          Vector(make.drop(11), color.drop(10), issuePrecinct.drop(6), precinct.drop(6)) -> "vehicle_make/2048;vehicle_color/1024;issuer_precinct/64;violation_precinct/64", //Q5
          Vector(squad.drop(2), lawsect, county) -> "issuer_squad/4;law_section;violation_county", //Q6
          Vector(year.drop(3), body.drop(8), agency) -> "issue_date_year/8;vehicle_body_type/256;issuing_agency", //Q7
          Vector(ptype.drop(3), state.drop(3), code.drop(3)) -> "plate_type/8;registration_state/8;violation_code/8;", //Q8
        )
    }

    (0 until numIters).foreach { i =>
      queries.foreach { case (queryDims, qname) =>
        val query = queryDims.reduce(_ ++ _).sorted
        val qs = query.size
        val trueResult = dc.naive_eval(query)
        expt.run(dc, dc.cubeName, query, trueResult, qname = qname + s"($qs-D)")
      }
    }
  }

  def manual_solvers(cg: CubeGenerator, isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val (minD, maxD) = cg match {
      case n: NYC => (18, 40)
      case s: SSB => (14, 30)
    }
    val logN = 15
    val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
    dc.loadPrimaryMoments(cg.baseName)
    val ename = s"${cg.inputname}-$isSMS-manual-solvers"
    val expt = new AllSolverOnlineExperiment(ename)
    expt.shouldRunNaive = true
    expt.shouldRunLP = false
    expt.shouldRunMoment = true
    expt.shouldRunIPF = true
    val sch = cg.schemaInstance
    val encMap = sch.columnVector.map(c => c.name -> c.encoder).toMap[String, ColEncoder[_]]
    val queries = cg match {
      case s: SSB =>
        val brand = encMap("brand").asInstanceOf[LazyMemCol].bits
        val snation = encMap("supp_nation").asInstanceOf[LazyMemCol].bits
        val cregion = encMap("cust_region").asInstanceOf[LazyMemCol].bits
        val scity = encMap("supp_city").asInstanceOf[LazyMemCol].bits
        val mfgr = encMap("mfgr").asInstanceOf[LazyMemCol].bits
        val tax = encMap("tax").asInstanceOf[StaticNatCol].bits
        Vector(
          Vector(brand.drop(1), snation, cregion) -> "brand/2;s_nation;cust_region", //Q1
//          Vector(scity, mfgr, tax) -> "s_city;mfgr;tax" //Q2
        )

      case n: NYC =>
        val year = encMap("Issue Date").asInstanceOf[StaticDateCol].yearCol.bits
        val issuePrecinct = encMap("Issuer Precinct").asInstanceOf[LazyMemCol].bits
        val state = encMap("Registration State").asInstanceOf[LazyMemCol].bits
        val body = encMap("Vehicle Body Type").asInstanceOf[LazyMemCol].bits
        val make = encMap("Vehicle Make").asInstanceOf[LazyMemCol].bits
        val ptype = encMap("Plate Type").asInstanceOf[LazyMemCol].bits
        val agency = encMap("Issuing Agency").asInstanceOf[LazyMemCol].bits
        val squad = encMap("Issuer Squad").asInstanceOf[LazyMemCol].bits
        val precinct = encMap("Violation Precinct").asInstanceOf[LazyMemCol].bits
        val color = encMap("Vehicle Color").asInstanceOf[LazyMemCol].bits
        val lawsect = encMap("Law Section").asInstanceOf[LazyMemCol].bits
        val code = encMap("Violation Code").asInstanceOf[LazyMemCol].bits
        val county = encMap("Violation County").asInstanceOf[LazyMemCol].bits
        Vector(
          Vector(make.drop(11), color.drop(10), issuePrecinct.drop(6), precinct.drop(6)) -> "vehicle_make/2048;vehicle_color/1024;issuer_precinct/64;violation_precinct/64", //Q5
//          Vector(squad.drop(2), lawsect, county) -> "issuer_squad/4;law_section;violation_county" //Q6
        )
    }


    queries.foreach { case (queryDims, qname) =>
      val query = queryDims.reduce(_ ++ _).sorted
      val qs = query.size
      val trueResult = dc.naive_eval(query)
      (0 until numIters).foreach { i => //same query run back to back
        expt.run(dc, dc.cubeName, query, trueResult, qname = qname + s"($qs-D)")
      }
    }
  }

  def manual_matparams(cg: CubeGenerator, isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val params = cg match {
      case n: NYC => Vector((15, 18, 40), (15, 14, 40), (15, 10, 40), (12, 18, 40), (9, 18, 40))
      case s: SSB => Vector((15, 14, 30), (15, 10, 30), (15, 6, 30), (12, 14, 30), (9, 14, 30))
    }
    val ename = s"${cg.inputname}-$isSMS-manual-matparams"
    val expt = new AllSolverOnlineExperiment(ename)
    expt.shouldRunNaive = false
    expt.shouldRunLP = false
    expt.shouldRunMoment = false
    expt.shouldRunIPF = true
    val sch = cg.schemaInstance
    val encMap = sch.columnVector.map(c => c.name -> c.encoder).toMap[String, ColEncoder[_]]
    val queries = cg match {

      case s: SSB =>
        val date = encMap("order_date").asInstanceOf[StaticDateCol]
        val brand = encMap("brand").asInstanceOf[LazyMemCol].bits
        val snation = encMap("supp_nation").asInstanceOf[LazyMemCol].bits
        val cregion = encMap("cust_region").asInstanceOf[LazyMemCol].bits
        Vector(
          Vector(brand.drop(1), snation, cregion) -> "brand/2;s_nation;cust_region" //Q1
        )
      case n: NYC =>
        val issuePrecinct = encMap("Issuer Precinct").asInstanceOf[LazyMemCol].bits
        val make = encMap("Vehicle Make").asInstanceOf[LazyMemCol].bits
        val color = encMap("Vehicle Color").asInstanceOf[LazyMemCol].bits
        val precinct = encMap("Violation Precinct").asInstanceOf[LazyMemCol].bits
        Vector(
          Vector(make.drop(11), color.drop(10), issuePrecinct.drop(6), precinct.drop(6)) -> "vehicle_make/2048;vehicle_color/1024;issuer_precinct/64;violation_precinct/64" //Q5
        )
    }


    params.foreach { case (logn, mind, maxd) =>
      val dc = cg.loadSMS(logn, mind, maxd)
      dc.loadPrimaryMoments(cg.baseName)
      queries.foreach { case (queryDims, qname) =>
        val query = queryDims.reduce(_ ++ _).sorted
        val qs = query.size
        val trueResult = dc.naive_eval(query)
        (0 until numIters).foreach { i => //same query run back to back
          expt.run(dc, dc.cubeName, query, trueResult, qname = qname + s"($qs-D)")
        }
      }
      dc.backend.reset
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

        case "manual-nyc-prefix-queries" => manual_queries(nyc, true)
        case "manual-ssb-prefix-queries" => manual_queries(ssb, true)

        case "manual-nyc-prefix-solvers" => manual_solvers(nyc, true)
        case "manual-ssb-prefix-solvers" => manual_solvers(ssb, true)

        case "manual-nyc-prefix-matparams" => manual_matparams(nyc, true)
        case "manual-ssb-prefix-matparams" => manual_matparams(ssb, true)
        case s => throw new IllegalArgumentException(s)
      }
    }

    run_expt(func)(args)
  }
}


