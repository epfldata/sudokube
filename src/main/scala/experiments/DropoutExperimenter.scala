package experiments

import backend.CBackend
import core.{MaterializedQueryResult, PartialDataCube}
import frontend.generators.{CubeGenerator, NYC, SSB}

/**
 * The experiment to drop out one cuboid at a time and record the resulting time, error, and entropy.
 * @author Zhekai Jiang
 */
object DropoutExperimenter {
  implicit val backend = CBackend.default
  def runDropoutExperiment(isSMS: Boolean, cubeGenerator: String, minNumDimensions: Int, querySize: Int, queryIndex: Int, policy: String)(implicit shouldRecord: Boolean): Unit = {
    val cg: CubeGenerator = if (cubeGenerator == "NYC")  NYC() else SSB(100)
    val param = s"15_${minNumDimensions}_30"
    val ms = if (isSMS) "sms3" else "rms3"
    val name = s"_${ms}_$param"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load(fullname, cg.baseName)
    dc.loadPrimaryMoments(cg.baseName)

    val expname2 = s"${policy.toLowerCase}-based-dropout-$cubeGenerator-$ms-dmin-$minNumDimensions-querydim-$querySize-query-$queryIndex"
    val exptfull = new DropoutExpt(expname2, policy)
    val materializedQueries = new MaterializedQueryResult(cg)
    val queries = materializedQueries.loadQueries(querySize)
    println(s"Dropout Experiment for ${cg.inputname} dataset MS = $ms (d_min = $minNumDimensions) Query Dimensionality = $querySize Query Number $queryIndex")
    val trueResult = materializedQueries.loadQueryResult(querySize, queryIndex)
    exptfull.run(dc, fullname, queries(queryIndex), trueResult, sliceValues = Vector())
    dc.cuboids.head.backend.reset
  }

  def main(args: Array[String]): Unit = {
    implicit val shouldRecord: Boolean = true

    runDropoutExperiment(isSMS = true, cubeGenerator = "SSB", minNumDimensions = 14, querySize = 18, queryIndex = 0, "InvNormalized-Entropy")
    runDropoutExperiment(isSMS = true, cubeGenerator = "SSB", minNumDimensions = 14, querySize = 18, queryIndex = 0, "Normalized-Entropy")
    runDropoutExperiment(isSMS = true, cubeGenerator = "SSB", minNumDimensions = 14, querySize = 18, queryIndex = 0, "Dimension")

    runDropoutExperiment(isSMS = true, cubeGenerator = "SSB", minNumDimensions = 14, querySize = 18, queryIndex = 12, "InvNormalized-Entropy")
    runDropoutExperiment(isSMS = true, cubeGenerator = "SSB", minNumDimensions = 14, querySize = 18, queryIndex = 12, "Normalized-Entropy")
    runDropoutExperiment(isSMS = true, cubeGenerator = "SSB", minNumDimensions = 14, querySize = 18, queryIndex = 12, "Dimension")

    runDropoutExperiment(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 14, querySize = 18, queryIndex = 0, "InvNormalized-Entropy")
    runDropoutExperiment(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 14, querySize = 18, queryIndex = 0, "Normalized-Entropy")
    runDropoutExperiment(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 14, querySize = 18, queryIndex = 0, "Dimension")

    runDropoutExperiment(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 14, querySize = 18, queryIndex = 12, "InvNormalized-Entropy")
    runDropoutExperiment(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 14, querySize = 18, queryIndex = 12, "Normalized-Entropy")
    runDropoutExperiment(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 14, querySize = 18, queryIndex = 12, "Dimension")

    runDropoutExperiment(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 6, querySize = 21, queryIndex = 0, "invNormalized-Entropy")
    runDropoutExperiment(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 6, querySize = 21, queryIndex = 0, "Normalized-Entropy")
    runDropoutExperiment(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 6, querySize = 21, queryIndex = 0, "Dimension")

    runDropoutExperiment(isSMS = true, cubeGenerator = "SSB", minNumDimensions = 14, querySize = 21, queryIndex = 0, "InvNormalized-Entropy")
    runDropoutExperiment(isSMS = true, cubeGenerator = "SSB", minNumDimensions = 14, querySize = 21, queryIndex = 0, "Normalized-Entropy")
    runDropoutExperiment(isSMS = true, cubeGenerator = "SSB", minNumDimensions = 14, querySize = 21, queryIndex = 0, "Dimension")

    runDropoutExperiment(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 14, querySize = 21, queryIndex = 0, "invNormalized-Entropy")
    runDropoutExperiment(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 14, querySize = 21, queryIndex = 0, "Normalized-Entropy")
    runDropoutExperiment(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 14, querySize = 21, queryIndex = 0, "Dimension")

    runDropoutExperiment(isSMS = true, cubeGenerator = "SSB", minNumDimensions = 14, querySize = 21, queryIndex = 1, "invNormalized-Entropy")
    runDropoutExperiment(isSMS = true, cubeGenerator = "SSB", minNumDimensions = 14, querySize = 21, queryIndex = 1, "Normalized-Entropy")
    runDropoutExperiment(isSMS = true, cubeGenerator = "SSB", minNumDimensions = 14, querySize = 21, queryIndex = 1, "Dimension")

    runDropoutExperiment(isSMS = true, cubeGenerator = "SSB", minNumDimensions = 14, querySize = 21, queryIndex = 5, "invNormalized-Entropy")
    runDropoutExperiment(isSMS = true, cubeGenerator = "SSB", minNumDimensions = 14, querySize = 21, queryIndex = 5, "Normalized-Entropy")
    runDropoutExperiment(isSMS = true, cubeGenerator = "SSB", minNumDimensions = 14, querySize = 21, queryIndex = 5, "Dimension")

    runDropoutExperiment(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 6, querySize = 24, queryIndex = 0, "invNormalized-Entropy")
    runDropoutExperiment(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 6, querySize = 24, queryIndex = 0, "Normalized-Entropy")
    runDropoutExperiment(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 6, querySize = 24, queryIndex = 0, "Dimension")

    runDropoutExperiment(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 14, querySize = 24, queryIndex = 0, "invNormalized-Entropy")
    runDropoutExperiment(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 14, querySize = 24, queryIndex = 0, "Normalized-Entropy")
    runDropoutExperiment(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 14, querySize = 24, queryIndex = 0, "Dimension")


  }
}
