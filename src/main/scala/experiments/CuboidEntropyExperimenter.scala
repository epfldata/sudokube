package experiments

import core.solver.SolverTools
import core.{MaterializedQueryResult, PartialDataCube}
import frontend.generators.{CubeGenerator, NYC, SSB}
import util.BitUtils

import java.io.{File, PrintStream}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * An experiment to output fetched cuboids' entropies and the "relative" entropies (ratio of the entropy to the maximum possible entropy of the cuboid's dimensionality).
 * @author Zhekai Jiang
 */
object CuboidEntropyExperimenter {
  def printCuboidEntropies(isSMS: Boolean, cubeGenerator: String, minNumDimensions: Int)(implicit shouldRecord: Boolean, numIters: Int): Unit = {
    val cg: CubeGenerator = if (cubeGenerator == "NYC") NYC else SSB(100)
    val param = s"15_${minNumDimensions}_30"
    val ms = if (isSMS) "sms3" else "rms3"
    val name = s"_${ms}_$param"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load(fullname, cg.baseName)
    dc.loadPrimaryMoments(cg.baseName)

    val materializedQueries = new MaterializedQueryResult(cg)
    val qss = List(6, 9, 12, 15, 18, 21)
    qss.foreach { qs =>
      val queries = materializedQueries.loadQueries(qs).take(numIters)
      println(s"Cuboid Entropy Experiment for ${cg.inputname} dataset, MS = $ms, Min Materialization Dimensionality = $minNumDimensions, Query Dimensionality = $qs")
      val ql = queries.length
      queries.zipWithIndex.foreach { case (qu, i) =>
        println(s"\tBatch Query ${i + 1}/$ql")

        val fileout = {
          val isFinal = true
          val (timestamp, folder) = {
            if (isFinal) ("final", ".")
            else if (shouldRecord) {
              val datetime = LocalDateTime.now
              (DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").format(datetime), DateTimeFormatter.ofPattern("yyyyMMdd").format(datetime))
            } else ("dummy", "dummy")
          }
          val file = new File(s"expdata/$folder/cuboid-entropy_$cubeGenerator-$ms-queryDim-$qs-minDim-$minNumDimensions-query-${i + 1}_$timestamp.csv")
          if (!file.exists())
            file.getParentFile.mkdirs()
          new PrintStream(file)
        }

        fileout.println("dimensions, variables, entropy, maxEntropy, relativeEntropy")

        val q = qu.sorted

        val (l, _) = dc.index.prepareBatch(q) -> SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
        val fetched = l.map { pm => (pm.queryIntersection, dc.fetch2[Double](List(pm))) }

        fetched.foreach { case (bits, array) =>
          val numDimensions = BitUtils.sizeOfSet(bits)
          val cuboidEntropy = SolverTools.entropy(array)
          val maxEntropy = -math.log(1.0 / (1 << numDimensions))
          val relativeEntropy = cuboidEntropy / maxEntropy
          fileout.println(s"$numDimensions, ${BitUtils.IntToSet(bits).mkString(":")}, $cuboidEntropy, $maxEntropy, $relativeEntropy")
        }

      }
    }
    dc.cuboids.head.backend.reset
  }

  def main(args: Array[String]): Unit = {
    implicit val shouldRecord: Boolean = true
    implicit val numIters: Int = 3
    printCuboidEntropies(isSMS = true, cubeGenerator = "SSB", minNumDimensions = 14)
    printCuboidEntropies(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 6)
    printCuboidEntropies(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 14)
    printCuboidEntropies(isSMS = false, cubeGenerator = "SSB", minNumDimensions = 14)
    printCuboidEntropies(isSMS = false, cubeGenerator = "NYC", minNumDimensions = 6)
    printCuboidEntropies(isSMS = false, cubeGenerator = "NYC", minNumDimensions = 14)
  }
}
