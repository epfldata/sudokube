package experiments

import core.{MaterializedQueryResult, PartialDataCube}
import frontend.generators.{CubeGenerator, NYC, SSB}

/**
 * Record the change of solution error as vanilla IPF runs (check once for each cuboid update).
 * This will show that IPF may have already converged even before finishing one iteration.
 * @author Zhekai Jiang
 */
object VanillaIPFErrorEvolutionExperimenter {
  def runVanillaIPFErrorExpt(isSMS: Boolean, cubeGenerator: String, minNumDimensions: Int, querySize: Int, queryIndex: Int)(implicit shouldRecord: Boolean): Unit = {
    val cg: CubeGenerator = if (cubeGenerator == "NYC") NYC else SSB(100)
    val param = s"15_${minNumDimensions}_30"
    val ms = if (isSMS) "sms3" else "rms3"
    val name = s"_${ms}_$param"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load(fullname, cg.baseName)
    dc.loadPrimaryMoments(cg.baseName)

    val expname2 = s"$cubeGenerator-$ms-dmin-$minNumDimensions-querydim-$querySize-query-$queryIndex"
    val exptfull = new VanillaIIPFErrorEvolutionExpt(expname2)
    val materializedQueries = new MaterializedQueryResult(cg)
    val queries = materializedQueries.loadQueries(querySize)
    println(s"Vanilla IPF Error Experiment for ${cg.inputname} dataset MS = $ms (d_min = $minNumDimensions) Query Dimensionality = $querySize Query Number $queryIndex")
    val trueResult = materializedQueries.loadQueryResult(querySize, queryIndex)

    exptfull.run(dc, fullname, queries(queryIndex), trueResult, sliceValues = Vector())
    dc.cuboids.head.backend.reset
  }

  def main(args: Array[String]): Unit = {
    implicit val shouldRecord: Boolean = true
    runVanillaIPFErrorExpt(isSMS = true, cubeGenerator = "SSB", minNumDimensions = 14, querySize = 18, queryIndex = 0)
    runVanillaIPFErrorExpt(isSMS = true, cubeGenerator = "SSB", minNumDimensions = 14, querySize = 21, queryIndex = 0)
    runVanillaIPFErrorExpt(isSMS = true, cubeGenerator = "SSB", minNumDimensions = 14, querySize = 21, queryIndex = 3)
    runVanillaIPFErrorExpt(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 6, querySize = 21, queryIndex = 0)
    runVanillaIPFErrorExpt(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 14, querySize = 21, queryIndex = 0)
    runVanillaIPFErrorExpt(isSMS = false, cubeGenerator = "SSB", minNumDimensions = 14, querySize = 18, queryIndex = 0)
    runVanillaIPFErrorExpt(isSMS = false, cubeGenerator = "NYC", minNumDimensions = 6, querySize = 21, queryIndex = 0)
    runVanillaIPFErrorExpt(isSMS = false, cubeGenerator = "NYC", minNumDimensions = 14, querySize = 21, queryIndex = 0)
  }
}
