package experiments

import backend.{Backend, Payload}
import core.DataCube
import frontend.experiments.Tools

import java.io.{File, PrintStream}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Random

abstract class Experiment(exptname: String, exptname2: String, dataSubfolder: String = ".")(implicit timestampedFolder: String = "latest") {
  val fileout = {
    val file = new File(s"expdata/$dataSubfolder/$timestampedFolder/${exptname2}_${exptname}.csv")
    if (!file.exists())
      file.getParentFile.mkdirs()
    new PrintStream(file)
  }

  def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean, qname: String = "", sliceValues: Seq[(Int, Int)]): Unit

  def warmup(nw: Int = 10)(implicit backend: Backend[Payload]) = {
    val name = "Warmup"
    val dcwarm = DataCube.load(name)
    dcwarm.loadPrimaryMoments(name)
    val qs = (0 to nw).map { i =>
      val s = Random.nextInt(4) + 4
      Tools.rand_q(dcwarm.index.n_bits, s)
    }

    qs.foreach(q => run(dcwarm, name, q, null, false, sliceValues = Vector()))
    println("Warmup Complete")
  }
}

object Experiment {
  def now() = {
    val formatter = DateTimeFormatter.ofPattern("yyyyMM/dd/HHmmss")
    formatter.format(LocalDateTime.now())
  }
}