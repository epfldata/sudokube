package experiments

import backend.CBackend
import core.{MaterializedQueryResult, PartialDataCube}
import frontend.generators.{CubeGenerator, NYC, SSB}

/**
 * Compare the time and error for IPF solvers and moment solver.
 * @author Zhekai Jiang
 */
object IPFExperimenter {
  implicit val backend = CBackend.original

  def ipf_clique_expt_qsize(isSMS: Boolean, cubeGenerator: String, minNumDimensions: Int)(implicit shouldRecord: Boolean, numIters: Int) = {
    val cg: CubeGenerator = if (cubeGenerator == "NYC") NYC() else SSB(100)
    val backendName = "original"
    val param = s"15_${minNumDimensions}_30"
    val ms = if (isSMS) "sms3" else "rms3"
    val name = s"_${ms}_$param"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load(fullname, cg.baseName)
    dc.loadPrimaryMoments(cg.baseName)

    val expname2 = s"query-dim-$cubeGenerator-$ms-dmin-$minNumDimensions-$backendName"
    val expt = new IPFCliqueExpt(expname2)
    if(shouldRecord) expt.warmup()
    val materializedQueries = new MaterializedQueryResult(cg)
    val qss = List(6, 9, 12, 15, 18, 21)
    qss.foreach { qs =>
      val queries = materializedQueries.loadQueries(qs).take(numIters)
      println(s"IPF Moment Stats Experiment for ${cg.inputname} dataset MS = $ms (d_min = $minNumDimensions) Query Dimensionality = $qs")
      val ql = queries.length
      queries.zipWithIndex.foreach { case (q, i) =>
        println(s"\tBatch Query ${i + 1}/$ql")
        val trueResult = materializedQueries.loadQueryResult(qs, i)
        expt.run(dc, fullname, q, trueResult, sliceValues = Vector())
      }
    }
    dc.cuboids.head.backend.reset
  }

  def ipf_clique_expt_matparams(isSMS: Boolean, cubeGenerator: String, qs: Int)(implicit shouldRecord: Boolean, numIters: Int) = {
    val cg: CubeGenerator = if (cubeGenerator == "NYC") NYC() else SSB(100)
    val backendName = "original"
    val ms = if (isSMS) "sms3" else "rms3"
    val maxD = 30
    val params = List(
      (13, 10),
      (15, 6), (15, 10), (15, 14),
      (17, 10)
    )
    val materializedQueries = new MaterializedQueryResult(cg)
    val queries = materializedQueries.loadQueries(qs).take(numIters)

    val expname2 = s"query-matparams-$cubeGenerator-$ms-qsize-$qs-$backendName"
    val expt = new IPFCliqueExpt(expname2)
    if (shouldRecord) expt.warmup()

    params.foreach { p =>
      val fullname = s"${cg.inputname}_${ms}_${p._1}_${p._2}_$maxD"
      val dc = PartialDataCube.load(fullname, cg.baseName)
      dc.loadPrimaryMoments(cg.inputname + "_base")
      println(s"IPF Moment Stats Experiment for ${cg.inputname} dataset MS = $ms Query Dimensionality = $qs NumCuboids=2^{${p._1}}, DMin=${p._2}")
      val ql = queries.length
      queries.zipWithIndex.foreach { case (q, i) =>
        println(s"\tBatch Query ${i + 1}/$ql")
        val trueResult = materializedQueries.loadQueryResult(qs, i)
        expt.run(dc, fullname, q, trueResult, sliceValues = Vector())
      }
      backend.reset
    }

  }

  def ipf_moment_compareTimeError(isSMS: Boolean, cubeGenerator: String, minNumDimensions: Int)(implicit shouldRecord: Boolean, numIters: Int): Unit = {
    val cg: CubeGenerator = if (cubeGenerator == "NYC") NYC() else SSB(100)
    val backendName = "original"
    val param = s"15_${minNumDimensions}_30"
    val ms = if (isSMS) "sms3" else "rms3"
    val name = s"_${ms}_$param"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load(fullname, cg.baseName)
    dc.loadPrimaryMoments(cg.baseName)

    val expname2 = s"query-dim-$cubeGenerator-$ms-dmin-$minNumDimensions-$backendName"
    val exptfull = new IPFMomentBatchExpt(expname2)
    if (shouldRecord) exptfull.warmup()
    val materializedQueries = new MaterializedQueryResult(cg)
    val qss = List(6, 9, 12, 15, 18, 21)
    qss.foreach { qs =>
      val queries = materializedQueries.loadQueries(qs).take(numIters)
      println(s"IPF Solvers vs Moment Solver Experiment for ${cg.inputname} dataset MS = $ms (d_min = $minNumDimensions) Query Dimensionality = $qs")
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
    implicit val numIters: Int = 20
    ipf_clique_expt_qsize(true, "NYC", minNumDimensions = 14)
    ipf_clique_expt_matparams(true, "NYC", qs = 18)
    //ipf_moment_compareTimeError(isSMS = true, cubeGenerator = "SSB", minNumDimensions = 14)
    //ipf_moment_compareTimeError(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 14)
    //ipf_moment_compareTimeError(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 6)
    //ipf_moment_compareTimeError(isSMS = false, cubeGenerator = "SSB", minNumDimensions = 14)
    //ipf_moment_compareTimeError(isSMS = false, cubeGenerator = "NYC", minNumDimensions = 14)
    //ipf_moment_compareTimeError(isSMS = false, cubeGenerator = "NYC", minNumDimensions = 6)
  }
}
