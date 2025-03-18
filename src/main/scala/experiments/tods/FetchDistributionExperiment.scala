package experiments.tods

import backend.CBackend
import core.{DataCube, MaterializedQueryResult}
import experiments.ExperimentRunner
import frontend.generators.{CubeGenerator, NYC, SSB}
import util.Profiler

class FetchDistributionExperiment(ename2: String = "")(implicit timestampedFolder: String, numIters: Int) extends SolverExperiment(s"fetch-batch", ename2) {
  val header = "CubeName,RunID,Query,QSize," +
    "CuboidDim,SrcCuboidCount,DstCuboidCount,SrcCuboidsTotalBytes"
  fileout.println(header)
  var maxDims = 40
  def prepare(dc: DataCube, query: IndexedSeq[Int]) = {
    val l = Profiler(s"Prepare") { // Not doing prepare for primary moments
      dc.index.prepareBatch(query)
    }
    l
  }
  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double] = null, output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Seq()): Unit = {
    val query = qu.sorted
    Profiler.resetAll()
    val prepared = prepare(dc, query)
    val srcCount = Array.fill(maxDims + 1)(0)
    val srcBytes = Array.fill(maxDims + 1)(0L)
    val dstCount = Array.fill(maxDims + 1)(0)
    prepared.foreach { pm =>
      val dstSize = pm.queryIntersectionSize
      val srcSize = pm.cuboidCost
      srcCount(srcSize) += 1
      srcBytes(srcSize) += dc.cuboids(pm.cuboidID).numBytes
      dstCount(dstSize) += 1
    }
    (0 to maxDims).foreach { k =>
      val row = s"$dcname,$runID,${qu.mkString(";")},${qu.size}," +
        s"$k,${srcCount(k)},${dstCount(k)},${srcBytes(k)}"
      fileout.println(row)
    }
    runID += 1
  }
}

object FetchDistributionExperiment extends ExperimentRunner {
  def qsize(cg: CubeGenerator, isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val (minD, maxD) = cg match {
      case n: NYC => (18, 40)
      case s: SSB => (14, 30)
    }
    val logN = 15
    val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
    val ename = s"${cg.inputname}-$isSMS-qsize"
    val expt = new FetchDistributionExperiment(ename)
    val mqr = new MaterializedQueryResult(cg, isSMS)
    Vector(2, 4, 6, 8, 10, 12, 14, 18).reverse.map { qs =>
      val queries = mqr.loadQueries(qs).take(numIters)
      queries.zipWithIndex.foreach { case (q, qidx) =>
        expt.run(dc, dc.cubeName, q)
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

        case "qsize-nyc-prefix" => qsize(nyc, true)
        case "qsize-nyc-random" => qsize(nyc, false)

        case s => throw new IllegalArgumentException(s)
      }
    }
    run_expt(func)(args)
  }
}