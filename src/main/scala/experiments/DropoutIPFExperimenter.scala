package experiments

import core.{MaterializedQueryResult, PartialDataCube}
import frontend.generators.{CubeGenerator, NYC, SSB}

/**
 * The experiment to drop out one cuboid at a time and record the resulting time, error, and entropy.
 * @author Zhekai Jiang
 */
object DropoutIPFExperimenter {
  def runDropoutExperiment(isSMS: Boolean, cubeGenerator: String, minNumDimensions: Int, querySize: Int, queryIndex: Int)(implicit shouldRecord: Boolean): Unit = {
    val cg: CubeGenerator = if (cubeGenerator == "NYC") NYC else SSB(100)
    val param = s"15_${minNumDimensions}_30"
    val ms = if (isSMS) "sms3" else "rms3"
    val name = s"_${ms}_$param"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load(fullname, cg.baseName)
    dc.loadPrimaryMoments(cg.baseName)

    val expname2 = s"dropout-ipf-$cubeGenerator-$ms-dmin-$minNumDimensions-querydim-$querySize-query-$queryIndex"
    val exptfull = new DropoutIPFExpt(expname2)
    val materializedQueries = new MaterializedQueryResult(cg)
    val queries = materializedQueries.loadQueries(querySize)
    println(s"Dropout IPF Experiment for ${cg.inputname} dataset MS = $ms (d_min = $minNumDimensions) Query Dimensionality = $querySize Query Number $queryIndex")
    val trueResult = materializedQueries.loadQueryResult(querySize, queryIndex)
    exptfull.run(dc, fullname, queries(queryIndex), trueResult, sliceValues = Vector())
    dc.cuboids.head.backend.reset
  }

  def main(args: Array[String]): Unit = {
    implicit val shouldRecord: Boolean = true
    runDropoutExperiment(isSMS = true, cubeGenerator = "SSB", minNumDimensions = 14, querySize = 21, queryIndex = 3)
  }
}
