package experiments

import core.DataCube
import frontend.experiments.Tools

import java.io.{File, PrintStream}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Random

abstract class Experiment(exptname: String, exptname2: String, dataSubfolder: String = ".")(implicit shouldRecord: Boolean) {
  val fileout = {
    val isFinal = true
    val (timestamp, folder) = {
      if (isFinal) ("final", f"$dataSubfolder/.")
      else if (shouldRecord) {
        val datetime = LocalDateTime.now
        (DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").format(datetime), dataSubfolder + "/" + DateTimeFormatter.ofPattern("yyyyMMdd").format(datetime))
      } else ("dummy", "dummy")
    }
    val file = new File(s"expdata/$folder/${exptname2}_${exptname}_${timestamp}.csv")
    if (!file.exists())
      file.getParentFile.mkdirs()
    new PrintStream(file)
  }

  def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean, qname: String = "", sliceValues: IndexedSeq[Int]): Unit

  def warmup(nw: Int = 10) = {
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
