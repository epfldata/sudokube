package experiments.thesis.materialization

import backend.CBackend
import combinatorics.Combinatorics
import core.DataCube
import experiments.{Experiment, ExperimentRunner}
import frontend.generators.{CubeGenerator, NYC, SSB}

class CuboidDistribution(maxD: Int, ename2: String)(implicit timestampedFolder: String) extends Experiment(ename2, s"cuboid-distr", "thesis/materialization") {
  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean, qname: String, sliceValues: Seq[(Int, Int)]): Unit = ???

  val header = (0 to maxD).mkString("Series,", ",", "")
  fileout.println(header)

  def n_proj_d(logN: Int, minD: Int)(d: Int) = if (d >= minD && d <= maxD && d <= (logN - 1 + minD)) {
    val n = math.pow(2, logN - 1 + minD - d)
    //    if (n < total) n.toInt else total.toInt //Assume total is greater
    n.toInt
  } else 0

  def getStats(cg: CubeGenerator, params: Vector[(Int, Int)]) = {
    val sch = cg.schemaInstance
    val totalPrefix = sch.root.numPrefixUpto(maxD).map(_.toDouble)
    val totalRandom = (0 to maxD).map { i => Combinatorics.comb(sch.n_bits, i).toDouble }.toList
    fileout.println("Total Random," + totalRandom.mkString(","))
    fileout.println("Total Prefix," + totalPrefix.mkString(","))

    params.foreach { case (logN, minD) =>
      val projs = (0 to maxD).map { i => n_proj_d(logN, minD)(i) }
      fileout.println(s"N=2^{$logN}\\ d_{\\min}=$minD," + projs.mkString(","))
    }
  }
}

object CuboidDistribution extends ExperimentRunner {
  def expt(cg: CubeGenerator, maxD: Int)(implicit  timestampFolder: String) = {
    val params = Vector((15, 10), (15, 14), (15, 18), (12, 14), (9, 14))
    val expt = new CuboidDistribution(maxD, cg.inputname)
    expt.getStats(cg, params)
  }

  def main(args: Array[String]) {
    implicit val be = CBackend.default
    val nyc = new NYC()
    val ssb = new SSB(100)

    def func(param: String)(timestamp: String, numIters: Int) = {
      implicit val ni = numIters
      implicit val ts = timestamp
      val nycMaxD = 40
      val ssbMaxD = 30
      param match {
        case "nyc" => expt(nyc, nycMaxD)
        case "ssb" => expt(ssb, ssbMaxD)
        case s => throw new IllegalArgumentException(s)
      }
    }

    run_expt(func)(args)
  }
}
