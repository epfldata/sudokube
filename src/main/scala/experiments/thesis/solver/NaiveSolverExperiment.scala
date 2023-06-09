package experiments.thesis.solver

import backend.CBackend
import core.{DataCube, MaterializedQueryResult}
import experiments.ExperimentRunner
import frontend.generators._
import util.Profiler

class NaiveSolverExperiment(ename2: String = "")(implicit timestampedFolder: String, numIters: Int) extends SolverExperiment(s"naive-solver", ename2) {
  val header = "CubeName,RunID,Query,QSize," +
    "PrepareTime(us),FetchTime(us),TotalTime(us)," +
    "SourceCuboidDim,SourceCuboidNumRows,SourceCuboidNumBytes"
  fileout.println(header)
  def runNaive(dc: DataCube, query: IndexedSeq[Int]) = {
    val l = Profiler("NaivePrepare") {
      dc.index.prepareNaive(query)
    }
    val res = Profiler("NaiveFetch") {
      dc.fetch(l).map(p => p.sm)
    }
    (l, res)
  }
  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Seq()): Unit = {
    val query = qu.sorted
    Profiler.resetAll()
    val (l, res) = runNaive(dc, query)
    val prepTime = Profiler.getDurationMicro("NaivePrepare")
    val fetchTime = Profiler.getDurationMicro("NaiveFetch")
    val totalTime = prepTime + fetchTime
    val projMD = l.head
    val cuboidDim = projMD.cuboidCost
    val cuboid = dc.cuboids(projMD.cuboidID)
    val cuboidNumRows = cuboid.size
    val cuboidNumBytes = cuboid.numBytes
    fileout.println(s"$dcname,$runID,${qu.mkString(";")},${qu.size}," +
      s"$prepTime,$fetchTime,$totalTime," +
      s"$cuboidDim,$cuboidNumRows,$cuboidNumBytes")
    runID += 1
  }
}

object NaiveSolverExperiment extends ExperimentRunner {
  def qsize(cg: CubeGenerator, isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val (minD, maxD) = cg match {
      case n: NYC => (18, 40)
      case s: SSB => (14, 30)
    }
    val logN = 15
    val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
    val ename = s"${cg.inputname}-$isSMS-qsize"
    val expt = new NaiveSolverExperiment(ename)
    val mqr = new MaterializedQueryResult(cg, isSMS)
    Vector(2, 4, 6, 8, 10, 12).reverse.map { qs =>
      val queries = mqr.loadQueries(qs).take(numIters)
      queries.foreach { q =>
        expt.run(dc, dc.cubeName, q, null)
      }
    }
    be.reset
  }

  def minD(cg: CubeGenerator, isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val qsize = if(isSMS) 6 else 4
    val (minDLast, maxD) = cg match {
      case n: NYC => (18, 40)
      case s: SSB => (14, 30)
    }
    val logN = 15

    val ename = s"${cg.inputname}-$isSMS-minD"
    //we allow queries to repeat.
    val mqr = new MaterializedQueryResult(cg, isSMS)
    val queries = mqr.loadQueries(qsize).take(numIters)
    val expt = new NaiveSolverExperiment(ename)
    (6 to minDLast).by(4).reverse.map { minD =>
      val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
      queries.foreach { q =>
        expt.run(dc, dc.cubeName, q, null)
      }
      be.reset
    }
  }

  def logN(cg: CubeGenerator, isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val qsize = if(isSMS) 6 else 4
    val (minD, maxD) = cg match {
      case n: NYC => (18, 40)
      case s: SSB => (14, 30)
    }

    val ename = s"${cg.inputname}-$isSMS-logN"
    //we allow queries to repeat.
    val mqr = new MaterializedQueryResult(cg, isSMS)
    val queries = mqr.loadQueries(qsize).take(numIters)
    val expt = new NaiveSolverExperiment(ename)
    (6 to 15).by(3).reverse.map { logN =>
      val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
      queries.foreach { q =>
        expt.run(dc, dc.cubeName, q, null)
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
