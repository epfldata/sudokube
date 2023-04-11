package core.solver.wavelet

import core.solver.SolverTools.error
import core.solver.iterativeProportionalFittingSolver.IPFUtils
import util.BitUtils.SetToInt

import scala.util.Random

object Demo {

  /** Example for Wavelet Solver */
  def main(args: Array[String]): Unit = {
    val rng = new Random(42)

    // Number of dimensions
    val querySize = 2

    // Generate random result
    val actual: Array[Double] = Array.fill(1 << querySize)(rng.nextInt(100))

    val solver = new SingleCuboidWaveletSolver(querySize)

    // List of cuboids represented as (column indices -> flattened array of values)
    val cuboids: Map[Seq[Int], Array[Double]] =
      Seq(
        Seq(0),
      ).map(columnIndices =>
        columnIndices -> IPFUtils.getMarginalDistribution(querySize, actual, columnIndices.size, SetToInt(columnIndices))
      ).toMap

    cuboids.foreach { case (columnIndices, values) =>
      println(s"Cuboid (${columnIndices.mkString(",")}): [ ${values.mkString(", ")} ]")
    }

    // Add cuboids to solver
    cuboids.foreach { case (columnIndices, values) => solver.addCuboid(columnIndices, values) }

    // Solve
    val result: Array[Double] = solver.solve()

    // print list of doubles with 2 decimal places
    println(s"Actual: ${PrintUtils.toString(actual)}")
    println(s"Result: ${PrintUtils.toString(result)}")
    println(s"Error: ${error(actual, result)}")
  }
}

object Demo2 {

  def main(args: Array[String]): Unit = {
    val t = new HaarTransformer[Double]()

    val a = Array(1.0, 5.0, 11.0, 2.0)
    println(s"Original: ${PrintUtils.toString(a)}")

    val w = t.forward(a)
    println(s"Wavelet: ${PrintUtils.toString(w)}")

    val a_ = t.reverse(w)
    println(s"Reversed: ${PrintUtils.toString(a_)}")
  }
}

object Demo3 {
  def main(args: Array[String]): Unit = {
    val solver3 = new SingleCuboidWaveletSolver(3, debug = true)
    val ca = (Seq(0, 2), Array(14.0, 20.0, 5.0, 20.0))
    solver3.addCuboid(ca._1, ca._2)
    solver3.solve()

    println("\n================================\n")

    val solver2 = new SingleCuboidWaveletSolver(3, debug = true)
    val cb = (Seq(0, 1), Array(5.0, 26.0, 14.0, 14.0))
    solver2.addCuboid(cb._1, cb._2)
    solver2.solve()

    println("\n================================\n")

    val solver = new SingleCuboidWaveletSolver(3, debug = true)
    val cba = (Seq(0, 1, 2), Array(1.0, 19.0, 13.0, 1.0, 4.0, 7.0, 1.0, 13.0))
    solver.addCuboid(cba._1, cba._2)
    solver.solve()
  }
}