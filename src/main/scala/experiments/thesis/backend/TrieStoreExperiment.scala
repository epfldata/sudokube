package experiments.thesis.backend

import core.{DataCube, MaterializedQueryResult}
import experiments.{Experiment, ExperimentRunner}
import backend.CBackend
import core.solver.SolverTools
import core.solver.moment.CoMoment5SolverDouble
import frontend.generators.{CubeGenerator, NYC, SSB}
import util.Profiler

class TrieStoreExperiment(ename2: String = "")(implicit timestampedFolder: String, numIters: Int) extends Experiment(ename2, s"trie-store-expt", "thesis/backend") {
  var runID = 0
  val header = "CubeName,RunID,Query,QSize," +
    "Prepare,Fetch,Solve,Total,Error," +
    "TriePrepareFetch,TrieSolve,TrieTotal,TrieError"
  fileout.println(header)

  def runTrie(dc: DataCube, query: IndexedSeq[Int], trueResult: Array[Double]) = {
    val qsize = query.size
    Profiler.resetAll()
    val solver = Profiler("Trie PrepareFetch") {
      val sliceArgs = Array.fill(qsize)(-1)
      val pm = SolverTools.preparePrimaryMomentsForQuery[Double](query, dc.primaryMoments)
      val s = new CoMoment5SolverDouble(qsize, true, null, pm)
      s.moments = CBackend.triestore.prepareFromTrie(query, sliceArgs)
      s
    }

    val result = Profiler("Trie Solve") {
      solver.fillMissing()
      solver.solve(true)
    }

    val prepareTime = Profiler.getDurationMicro("Trie PrepareFetch")
    val solveTime = Profiler.getDurationMicro("Trie Solve")
    val error = SolverTools.error(trueResult, result)
    val totalTime = prepareTime + solveTime
    s"$prepareTime,$solveTime,$totalTime,$error"
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
  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean=true, qname: String="", sliceValues: Seq[(Int, Int)]=Seq()): Unit = {
    val query = qu.sorted
    val common = s"$dcname,$runID,${qu.mkString(";")},${qu.size}"
    val cuboid = runMoment(dc, query, trueResult)
    val trie = runTrie(dc, query, trueResult)
    val row = s"$common,$cuboid,$trie"
    fileout.println(row)
    runID += 1
  }
}

object TrieStoreExperiment extends ExperimentRunner {
  def qsize(cg: CubeGenerator, isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val (minD, maxD) = cg match {
      case n: NYC => (18, 40)
      case s: SSB => (14, 30)
    }
    val logN = 15
    val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
    dc.loadPrimaryMoments(cg.baseName)
    CBackend.triestore.loadTrie(s"cubedata/triestore/${dc.cubeName}.trie")
    val ename = s"${cg.inputname}-$isSMS"
    val expt = new TrieStoreExperiment(ename)
    val mqr = new MaterializedQueryResult(cg, isSMS)
    Vector(6, 8, 10, 12).reverse.map { qs =>
      val queries = mqr.loadQueries(qs).take(numIters)
      queries.zipWithIndex.foreach { case (q, qidx) =>
        val trueResult = mqr.loadQueryResult(qs, qidx)
        expt.run(dc, dc.cubeName, q, trueResult)
      }
    }
    be.reset
  }
  def main(args: Array[String]) {
    implicit val be = CBackend.default
    val nyc = new NYC()
    val ssb = new SSB(100)

    def func(param: String)(timestamp: String, numIters: Int) = {
      implicit val ni = numIters
      implicit val ts = timestamp
      param match {

        case "nyc-prefix-qsize" => qsize(nyc, true)
        case "nyc-random-qsize" => qsize(nyc, false)

        case "ssb-prefix-qsize" => qsize(ssb, true)
        case "ssb-random-qsize" => qsize(ssb, false)

        case s => throw new IllegalArgumentException(s)
      }
    }
    run_expt(func)(args)
  }

}
