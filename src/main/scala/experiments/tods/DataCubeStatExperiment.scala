package experiments.tods

import _root_.backend.CBackend
import core.DataCube
import experiments.{Experiment, ExperimentRunner}
import frontend.generators.{CubeGenerator, NYC, SSB}


class DataCubeStatExperiment(ename2: String)(implicit timestampedFolder: String) extends Experiment(ename2, s"cube-stat",  "tods23") {
  val header = "Dataset,MS,logn,mind,baseGB,cuboidGB,ratio"
  fileout.println(header)
  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int] = null, trueResult: Array[Double] = null, output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Seq()): Unit = {
    val baseSize = dc.cuboids.last.numBytes
    val baseGB = baseSize * math.pow(10, -9)
    val cuboidsSize = dc.cuboids.map(_.numBytes).sum //includes base data too
    val cuboidGB = cuboidsSize * math.pow(10, -9)
    val relativeSize = cuboidGB / baseGB
    val params = dcname.split("_")
    val cg = params(0)
    val ms = params(1)
    val logn = params(2)
    val mind = params(3)
    val row = s"${cg},$ms,$logn,$mind,$baseGB,$cuboidGB,$relativeSize"
    fileout.println(row)
  }
}

object DataCubeStatExperiment extends ExperimentRunner {
  def expt(cg: CubeGenerator, isSMS: Boolean)(implicit timestampedFolder: String, be: CBackend): Unit = {
    val params = cg match {
      case n: NYC =>
          Vector(
            (9, 18, 40), (12, 18, 40),
            (15, 18, 40),
            (15, 14, 40), (15, 10, 40)
          )

      case s: SSB =>
        val maxD = 30
        Vector(
          (9, 14, 30), (12, 14, 30),
          (15, 14, 30),
          (15, 10, 30), (15, 6, 30)
        )}

    val exptname = s"${cg.inputname}-$isSMS"
    val expt = new DataCubeStatExperiment(exptname)
    params.foreach { case (logn, mind, maxd) =>
      val dc = if (isSMS) cg.loadSMS(logn, mind, maxd)
      else cg.loadRMS(logn, mind, maxd)
      expt.run(dc, dc.cubeName)
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