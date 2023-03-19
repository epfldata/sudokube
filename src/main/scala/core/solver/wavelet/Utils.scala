package core.solver.wavelet

import core.solver.SolverTools.error
import core.solver.iterativeProportionalFittingSolver.IPFUtils
import util.BitUtils.SetToInt

import java.io.{File, PrintWriter}
import scala.util.Random

object AnalyseTransformer {
  def main(args: Array[String]): Unit = {
    val rng = new Random(42)

    val writer = new PrintWriter(new File("./src/main/scala/core/solver/wavelet/wavelet-analysis.md"))

    val transformer = new HaarTransformer[Double]()
    writer.write(s"## ${transformer.name}  \n")

    val querySizes = Seq(2, 3, 4, 5)
    querySizes.foreach { querySize =>
      val actual: Array[Double] = Array.fill(1 << querySize)(rng.nextInt(20))
      val actualWavelet: Array[Double] = transformer.forward(actual)

      // primary cuboid for querySize 3 is (0, 1, 2)
      val primaryCuboidShape: Seq[Int] = (0 until querySize)
      // all cuboids for querySize 3 are (0, 1), (0, 2), (1, 2), (0), (1), (2)
      val allCuboidShapes: Seq[Seq[Int]] = (1 until querySize).flatMap { n =>
        primaryCuboidShape.combinations(n).flatMap {
          cuboidOfSizeN => cuboidOfSizeN.permutations
        }
      }

      val allCuboids: Map[Seq[Int], Array[Double]] =
        allCuboidShapes.map(columnIndices =>
          columnIndices -> IPFUtils.getMarginalDistribution(querySize, actual, columnIndices.size, SetToInt(columnIndices))
        ).toMap

      val allWavelets = allCuboids.map { case (columnIndices, values) =>
        columnIndices -> (values, transformer.forward(values))
      }

      writer.write(s"### Query Size $querySize  \n")
      writer.write(s"    Primary: ${PrintUtils.toString(actual)}  \n")
      writer.write(s"    Wavelet: ${PrintUtils.toString(actualWavelet)}  \n")
      allWavelets.foreach { case (columnIndices, (cuboid, wavelet)) =>
        writer.write(s"a. ${columnIndices.mkString(",")}  \n")
        writer.write(s"    Original: ${PrintUtils.toString(cuboid)}  \n")
        writer.write(s"    Wavelet: ${PrintUtils.toString(wavelet)}  \n")
      }
    }
    writer.close()
  }
}

object Demo {

  /** Example for Wavelet Solver */
  def main(args: Array[String]): Unit = {
    val rng = new Random(42)

    // Number of dimensions
    val querySize = 3

    // Generate random result
    val actual: Array[Double] = Array.fill(1 << querySize)(rng.nextInt(100))

    val solver = new HaarWaveletSolver(querySize)

    // List of cuboids represented as (column indices -> flattened array of values)
    val cuboids: Map[Seq[Int], Array[Double]] =
      Seq(Seq(1, 2)).map(columnIndices =>
        columnIndices -> IPFUtils.getMarginalDistribution(querySize, actual, columnIndices.size, SetToInt(columnIndices))
      ).toMap

    // Add cuboids to solver
    cuboids.foreach { case (columnIndices, values) => solver.addCuboid(SetToInt(columnIndices), values) }

    // Solve
    val result: Array[Double] = solver.solve()

    // print list of doubles with 2 decimal places
    println(s"Actual: ${PrintUtils.toString(actual)}")
    println(s"Result: ${PrintUtils.toString(result)}")
    println(s"Error: ${error(actual, result)}")
  }
}

object PrintUtils {
  def toString(array: Array[Double], sep: String = " "): String =
    array.map { double => "%.2f".format(double) }.mkString(sep)
}
