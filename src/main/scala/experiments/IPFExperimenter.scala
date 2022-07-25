package experiments

import core.{MaterializedQueryResult, PartialDataCube}
import frontend.generators.{CubeGenerator, NYC, SSB}

object IPFExperimenter {
  def ipf_moment_compareTimeError(isSMS: Boolean, cubeGenerator: String, minNumDimensions: Int)(implicit shouldRecord: Boolean, numIters: Int): Unit = {
    val cg: CubeGenerator = if (cubeGenerator == "NYC") NYC else SSB(100)
    val param = s"15_${minNumDimensions}_30"
    val ms = if (isSMS) "sms3" else "rms3"
    val name = s"_${ms}_$param"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load(fullname, cg.baseName)
    dc.loadPrimaryMoments(cg.baseName)

    val expname2 = s"query-dim-$cubeGenerator-$ms"
    val exptfull = new IPFMomentBatchExpt(expname2)
    if (shouldRecord) exptfull.warmup()
    val materializedQueries = new MaterializedQueryResult(cg)
    val qss = List(6, 9, 12, 15, 18, 21)
    qss.foreach { qs =>
      val queries = materializedQueries.loadQueries(qs).take(numIters)
      println(s"Vanilla IPF vs Effective IPF vs Loopy IPF vs Moment Solver Experiment for ${cg.inputname} dataset MS = $ms Query Dimensionality = $qs")
      val ql = queries.length
      queries.zipWithIndex.foreach { case (q, i) =>
        println(s"\tBatch Query ${i + 1}/$ql")
        val trueResult = materializedQueries.loadQueryResult(qs, i)
        exptfull.run(dc, fullname, q, trueResult, sliceValues = Vector())
      }
    }
    dc.cuboids.head.backend.reset
  }

  def main(args: Array[String]): Unit = {
    implicit val shouldRecord: Boolean = false
    implicit val numIters: Int = 2
    ipf_moment_compareTimeError(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 6)
    ipf_moment_compareTimeError(isSMS = false, cubeGenerator = "NYC", minNumDimensions = 6)
    ipf_moment_compareTimeError(isSMS = true, cubeGenerator = "SSB", minNumDimensions = 14)
    ipf_moment_compareTimeError(isSMS = false, cubeGenerator = "SSB", minNumDimensions = 14)
    ipf_moment_compareTimeError(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 14)
    ipf_moment_compareTimeError(isSMS = false, cubeGenerator = "NYC", minNumDimensions = 14)
  }
}
