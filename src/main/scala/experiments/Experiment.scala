package experiments

import core.DataCube
import frontend.experiments.Tools

import java.io.{File, PrintStream}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Random

abstract class Experiment(dc_expt: DataCube, exptname: String, cubename: String)(implicit shouldRecord: Boolean) {
  var dc: DataCube = dc_expt
  val fileout = {
    val (timestamp,folder) = if(shouldRecord) {
      val datetime = LocalDateTime.now
      (DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").format(datetime), DateTimeFormatter.ofPattern("yyyyMMdd").format(datetime))
    } else ("dummy", "dummy")
    val file = new File(s"expdata/$folder/${exptname}_${cubename}_${timestamp}.csv")
    if(!file.exists())
      file.getParentFile.mkdirs()
    new PrintStream(file)
  }
  def run(qu: Seq[Int], output: Boolean): Unit
  def warmup(nw: Int = 100) = {
    val dcwarm = DataCube.load2("warmup")
    val qs = (0 to nw).map { i =>
      val s = Random.nextInt(4) + 4
      Tools.rand_q(dcwarm.m.n_bits, s)
    }
    dc = dcwarm
   qs.foreach(q => run(q, false))
    println("Warmup Complete")
    dc = dc_expt
  }
}
