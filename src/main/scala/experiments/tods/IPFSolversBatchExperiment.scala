package experiments.tods

import backend.CBackend
import core.solver.SolverTools._
import core.solver.iterativeProportionalFittingSolver.{EffectiveIPFSolver, MSTVanillaIPFSolver, NewVanillaIPFSolver, VanillaIPFSolver}
import core.{DataCube, MaterializedQueryResult}
import experiments.ExperimentRunner
import frontend.generators.{CubeGenerator, NYC, SSB}
import util.{BitUtils, Profiler}

class IPFSolversBatchExperiment(ename2: String = "")(implicit timestampedFolder: String, numIters: Int) extends SolverExperiment(s"ipf-batch", ename2) {
  val header = "CubeName,RunID,Query,QSize," +
    "PrepareTime(us),FetchTime(us)," +
    "DOF," +
    "SolveTimeVanilla(us),VanillaErrL1," +
    "SolveTimeJunction(us),JunctionIPFErrL1," +
    "SolveTimeMomentIPF(us),MomentIPFErrL1," +
    "SolveTimeMST(us),MSTErrL1"
  fileout.println(header)
  def runVanilla(query: IndexedSeq[Int], cuboids: Seq[(Int, Array[Double])]) = {
    Profiler("Solve Vanilla") {
      val solver = new VanillaIPFSolver(query.length)
      cuboids.foreach { case (qi, array) => solver.add(qi, array) }
      solver.solve()
      solver
    }
  }
  def runJunction(query: IndexedSeq[Int], cuboids: Seq[(Int, Array[Double])]) = {
    Profiler("Solve Junction") {
      val solver = new EffectiveIPFSolver(query.length)
      cuboids.foreach { case (qi, array) => solver.add(qi, array) }
      solver.solve()
      solver
    }
  }
  def runMoment(query: IndexedSeq[Int], cuboids: Seq[(Int, Array[Double])]) = {
    Profiler("Solve Moment") {
      val solver = new NewVanillaIPFSolver(query.length)
      cuboids.foreach { case (qi, array) => solver.add(qi, array) }
      solver.solve()
      solver
    }
  }
  def runMST(query: IndexedSeq[Int], cuboids: Seq[(Int, Array[Double])]) = {
    Profiler("Solve MST") {
      val solver = new MSTVanillaIPFSolver(query.length)
      cuboids.foreach { case (qi, array) => solver.add(qi, array) }
      solver.solve()
      solver
    }
  }
  def prepareAndFetch(dc: DataCube, query: IndexedSeq[Int]) = {
    val (l, pm) = Profiler(s"Prepare") { // Not doing prepare for primary moments
      dc.index.prepareBatch(query) -> preparePrimaryMomentsForQuery[Double](query, dc.primaryMoments)
    }
    val fetched = Profiler(s"Fetch") {
      val unionOfBits = l.map { _.queryIntersection }.reduce(_ | _)
      val total = pm.head._2
      val missing1Dmarginals = pm.tail.flatMap { case (h, m) =>
        if ((unionOfBits & h) == 0)
          Some(h -> Array(total - m, m))
        else None
      }
      missing1Dmarginals ++ l.map { pm => (pm.queryIntersection -> dc.fetch2[Double](List(pm))) }
    }
    l -> fetched
  }


  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Seq()): Unit = {
    val query = qu.sorted
    Profiler.resetAll()
    val (prepared, fetched) = prepareAndFetch(dc, query)

    val knownSet = collection.mutable.BitSet()
    prepared.foreach { pm =>
      val cols = pm.queryIntersection
      val n0 = (1 << pm.queryIntersectionSize)
      (0 until n0).map { i0 =>
        val i = BitUtils.unprojectIntWithInt(i0, cols)
        knownSet += i
      }
    }
    val dof = trueResult.size - knownSet.size
    val prepareTime = Profiler.getDurationMicro("Prepare")
    val fetchTime = Profiler.getDurationMicro("Fetch")

    val vanilla = runVanilla(query, fetched)
    val vanillaSolve = Profiler.getDurationMicro("Solve Vanilla")
    val vanillaErrorL1 = error(trueResult, vanilla.solution)


    val junction = runJunction(query, fetched)
    val junctionSolve = Profiler.getDurationMicro("Solve Junction")
    val junctionErrorL1 = error(trueResult, junction.solution)

    val momentIPF = runMoment(query, fetched)
    val momentSolve = Profiler.getDurationMicro("Solve Moment")
    val momentErrorL1 = error(trueResult, momentIPF.solution)

    val mstIPF = runMST(query, fetched)
    val mstSolve = Profiler.getDurationMicro("Solve MST")
    val mstErrorL1 = error(trueResult, mstIPF.solution)

    val row = s"$dcname,$runID,${qu.mkString(";")},${qu.size}," +
      s"$prepareTime,$fetchTime," +
      s"$dof,"+
      s"$vanillaSolve,$vanillaErrorL1," +
      s"$junctionSolve,$junctionErrorL1," +
      s"$momentSolve,$momentErrorL1," +
      s"$mstSolve,$mstErrorL1"
    fileout.println(row)
    runID += 1
  }
}

object IPFSolversBatchExperiment extends ExperimentRunner {
  def qsize(cg: CubeGenerator, isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val (minD, maxD) = cg match {
      case n: NYC => (18, 40)
      case s: SSB => (14, 30)
    }
    val logN = 15
    val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
    dc.loadPrimaryMoments(cg.baseName)
    val ename = s"${cg.inputname}-$isSMS-qsize"
    val expt = new IPFSolversBatchExperiment(ename)
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
  def main(args: Array[String]) = {
    implicit val be = CBackend.default
    val nyc = new NYC()
    val ssb = new SSB(100)

    def func(param: String)(timestamp: String, numIters: Int) = {
      implicit val ni = numIters
      implicit val ts = timestamp
      param match {

        case "qsize-ssb-prefix" => qsize(ssb, true)
        case "qsize-ssb-random" => qsize(ssb, false)

        case s => throw new IllegalArgumentException(s)
      }
    }
    run_expt(func)(args)
  }
}
