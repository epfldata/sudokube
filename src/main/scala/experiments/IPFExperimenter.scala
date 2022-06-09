package experiments

import core.PartialDataCube
import frontend.generators.{NYC, SSB}

object IPFExperimenter {
  def vanillaIPF_moment_compareTimeError(isSMS: Boolean)(implicit shouldRecord: Boolean, numIters: Int): Unit = {
    val cg = NYC
    val param = "15_6_30"
    val ms = if (isSMS) "sms3" else "rms3"
    val name = s"_${ms}_$param"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load2(fullname, cg.inputname + "_base")
    dc.loadPrimaryMoments(cg.inputname + "_base")
    val sch = cg.schema()

    val expname2 = s"query-dim-$ms"
    val exptfull = new VanillaIPFMomentBatchExpt(expname2)
    if (shouldRecord) exptfull.warmup()

    val qss = List(12, 15)
    qss.foreach { qs =>
      val queries = (0 until numIters).map(_ => sch.root.samplePrefix(qs)).distinct
      println(s"Vanilla IPF vs Moment Solver Experiment for MS = $ms Query Dimensionality = $qs")
      val ql = queries.length
      queries.zipWithIndex.foreach { case (q, i) =>
        println(s"\tBatch Query ${i + 1}/$ql")
        exptfull.run(dc, fullname, q)
      }
    }
    dc.cuboids.head.backend.reset
  }

  def main(args: Array[String]): Unit = {
    implicit val shouldRecord: Boolean = false
    implicit val numIters: Int = 5
    vanillaIPF_moment_compareTimeError(true)
    vanillaIPF_moment_compareTimeError(false)
  }
}
