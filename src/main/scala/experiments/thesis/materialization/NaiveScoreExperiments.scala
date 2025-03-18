package experiments.thesis.materialization

import backend.CBackend
import core.{DataCube, MaterializedQueryResult, PartialDataCube}
import experiments.{Experiment, ExperimentRunner}
import frontend.generators.{CubeGenerator, NYC, RandomCubeGenerator, SSBSample}
import util.Profiler

class NaiveScoreExperiments(ename2: String = "")(implicit timestampedFolder: String, numIters: Int) extends Experiment(ename2, s"naive-score", "thesis/materialization") {
  val header = "CubeName,RunID,Query,QSize," +
    "PrepareTime(us),FetchTime(us)," +
    "scoreNaive,error"
  fileout.println(header)
  var runID = 0

  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double] = null, output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Seq()): Unit = {
    val query = qu.sorted
    val qsize = qu.size
    Profiler.resetAll()
    val l = Profiler("NaivePrepare") {
      dc.index.prepareBatch(query) //Excludes base cuboid
    }
    val res = Profiler("NaiveFetch") {
      if (l.isEmpty) null
      else dc.fetch(l).map(p => p.sm)
    }
    val prepareTime = Profiler.getDurationMicro("NaivePrepare")
    val fetchTime = Profiler.getDurationMicro("NaiveFetch")
    val scoreNaive = if (l.isEmpty) 0.0 else 1.0
    val error = if (l.isEmpty) 1 else 0.0
    val row = s"$dcname,$runID,${qu.mkString(";")},$qsize," +
      s"$prepareTime,$fetchTime," +
      s"$scoreNaive,$error"
    fileout.println(row)
    runID += 1
  }
}

object NaiveScoreExperiments extends ExperimentRunner {
  implicit val be = CBackend.default
  def singlelevel(cg: CubeGenerator, b: Int, qs: Int)(implicit timestampedFolder: String, numIters: Int) = {
    val ename = s"${cg.inputname}_${b}_${qs}"
    val expt = new NaiveScoreExperiments(ename)
    val mqr = new MaterializedQueryResult(cg, false)
    val queries = mqr.loadQueries(qs).take(numIters)
    (2 to 20).foreach { k =>
      val dcname = s"${cg.inputname}_random_${b}_$k"
      val dc = PartialDataCube.load(dcname, cg.baseName)
      dc.loadPrimaryMoments(cg.baseName)
      if (dc.cuboids.length > 1) { //materialize at least one cuboid other than base
        queries.zipWithIndex.foreach { case (query, qidx) =>
          expt.run(dc, dc.cubeName, query)
        }
      }
    }
  }

  def main(args: Array[String]) = {
    val nyc = new NYC()
    val ssb = new SSBSample(20)
    val rand = new RandomCubeGenerator(100, 20)
    def func(param: String)(timestamp: String, numIters: Int) = {
      implicit val ni = numIters
      implicit val ts = timestamp
      val myargs = param.split("_")
      val cg = myargs(0) match {
        case "nyc" => nyc
        case "ssb" => ssb
        case "random" => rand
        case s => throw new IllegalArgumentException(s)
      }
      val b = myargs(1).toInt
      val qs = myargs(2).toInt
      singlelevel(cg, b, qs)
    }
    run_expt(func)(args)
  }
}