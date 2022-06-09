package experiments

import core.{MaterializedQueryResult, PartialDataCube}
import frontend.generators.{CubeGenerator, NYC, SSB}

object IPFExperimenter {
  def vanillaIPF_moment_compareTimeError(isSMS: Boolean, cubeGenerator: String)(implicit shouldRecord: Boolean, numIters: Int): Unit = {
    val cg: CubeGenerator = if (cubeGenerator == "NYC") NYC else SSB(100)
    val param = if (cubeGenerator == "NYC") "15_6_30" else "15_14_30"
    val ms = if (isSMS) "sms3" else "rms3"
    val name = s"_${ms}_$param"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load2(fullname, cg.inputname + "_base")
    dc.loadPrimaryMoments(cg.inputname + "_base")

    val expname2 = s"query-dim-$cubeGenerator-$ms"
    val exptfull = new VanillaIPFMomentBatchExpt(expname2)
    if (shouldRecord) exptfull.warmup()
    val materializedQueries = new MaterializedQueryResult(cg)
    val qss = List(12, 15)
    qss.foreach { qs =>
      val queries = materializedQueries.loadQueries(qs).take(numIters)
      println(s"Vanilla IPF vs Moment Solver Experiment for ${cg.inputname} dataset MS = $ms Query Dimensionality = $qs")
      val ql = queries.length
      queries.zipWithIndex.foreach { case (q, i) =>
        println(s"\tBatch Query ${i + 1}/$ql")
        val trueResult = materializedQueries.loadQueryResult(qs, i)
        exptfull.run(dc, fullname, q, trueResult)
      }
    }
    dc.cuboids.head.backend.reset
  }

  def main(args: Array[String]): Unit = {
    implicit val shouldRecord: Boolean = false
    implicit val numIters: Int = 2
    vanillaIPF_moment_compareTimeError(isSMS = true, cubeGenerator = "NYC")
    vanillaIPF_moment_compareTimeError(isSMS = false, cubeGenerator = "NYC")
    vanillaIPF_moment_compareTimeError(isSMS = true, cubeGenerator = "SSB")
    vanillaIPF_moment_compareTimeError(isSMS = false, cubeGenerator = "SSB")
  }
}
