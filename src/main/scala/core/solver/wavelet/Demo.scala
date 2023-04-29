package core.solver.wavelet

import core.solver.SolverTools
import core.solver.iterativeProportionalFittingSolver.IPFUtils
import util.BitUtils

import scala.util.Random

object Demo {

  /**
   * Run the solver with the given parameters, and print the actual data, calculated result and error
   *
   * @param cuboidIndices list of cuboids to provide to the solver written as Seq(marginal_variables)
   * @param solver        solver to use
   */
  def run(cuboidIndices: Seq[Seq[Int]], solver: WaveletSolver[Double]): Unit = {
    val rng = new Random(42)

    val querySize = solver.querySize

    // Generate random result
    val actual: Array[Double] = Array.fill(1 << querySize)(rng.nextInt(100))

    implicit def seqToInt: Seq[Int] => Int = BitUtils.SetToInt

    // List of cuboids represented as (column indices -> flattened array of values)
    val cuboids: Map[Seq[Int], Array[Double]] =
      cuboidIndices.map { marginalVariables =>
        marginalVariables -> IPFUtils.getMarginalDistribution(querySize, actual, marginalVariables.size, marginalVariables)
      }.toMap

    // Add cuboids to solver
    cuboids.foreach { case (columnIndices, values) => solver.addCuboid(columnIndices, values) }

    // Solve
    val result: Array[Double] = solver.solve()

    // print list of doubles with 2 decimal places
    println(s"Actual: ${PrintUtils.toString(actual)}")
    println(s"Result: ${PrintUtils.toString(result)}")
    println(s"Error: ${SolverTools.error(actual, result)}")
  }
}

object TransformDemo {

  def main(args: Array[String]): Unit = {
    val t = new HaarTransformer[Double]()

    val a = Array(1.0, 5.0, 11.0, 2.0)

    val w = t.forward(a)

    val a_ = t.reverse(w)

    println(s"Original: ${PrintUtils.toString(a)}")
    println(s"Wavelet: ${PrintUtils.toString(w)}")
    println(s"Reversed: ${PrintUtils.toString(a_)}")
  }
}


object Demo1 {
  def main(args: Array[String]): Unit = {
    Demo.run(
      Seq(Seq(0, 1, 4), Seq(1, 5), Seq(2, 3, 5), Seq(0, 2), Seq(3, 4), Seq(1)),
      new CoefficientSelectionWaveletSolver(6, debug = true))
  }
}

object Demo2 {
  def main(args: Array[String]): Unit = {
    Demo.run(
      Seq(Seq(0, 1, 4), Seq(1, 5), Seq(2, 3, 5), Seq(0, 2), Seq(3, 4), Seq(1)),
      new CoefficientAveragingWaveletSolver(6, debug = true))
  }
}

object Demo3 {

  def main(args: Array[String]): Unit = {

    val runs: Seq[Run] = Seq(
      //      Run(3, Seq((Seq(0, 1, 2), Array(1.0, 19.0, 13.0, 1.0, 4.0, 7.0, 1.0, 13.0))), new SingleCuboidWaveletSolver(_, _)),
      //      Run(3, Seq((Seq(0, 1), Array(5.0, 26.0, 14.0, 14.0))), new SingleCuboidWaveletSolver(_, _)),
      //      Run(3, Seq((Seq(0, 2), Array(14.0, 20.0, 5.0, 20.0))), new SingleCuboidWaveletSolver(_, _)),
      //      Run(3, Seq((Seq(1), Array(31.0, 28.0))), new SingleCuboidWaveletSolver(_, _)),
      //      Run(3, Seq((Seq(0), Array(19.0, 40.0))), new SingleCuboidWaveletSolver(_, _)),
      //      Run(3,
      //        Seq(
      //          (Seq(1), Array(31.0, 28.0)),
      //          (Seq(0, 2), Array(14.0, 20.0, 5.0, 20.0))
      //        ),
      //        new MutuallyExclusiveWaveletSolver(_, _)
      //      ),
      //      Run(3,
      //        Seq(
      //          (Seq(0, 1, 2), Array(1.0, 19.0, 13.0, 1.0, 4.0, 7.0, 1.0, 13.0)),
      //        ),
      //        new MutuallyExclusiveWaveletSolver(_, _)
      //      ),
      Run(3, Seq((Seq(0, 1, 2), Array(1.0, 5.0, 11.0, 7.0, 12.0, 4.0, 6.0, 3.0))), new SingleCuboidWaveletSolver(_, _)),


    )

    runs.foreach { case Run(querySize, cuboids, createSolver) =>
      val solver = createSolver(querySize, true)
      cuboids.foreach { case (columnIndices, values) =>
        solver.addCuboid(columnIndices, values)
      }
      solver.solve()

      println("================================")
    }
  }

  private case class Run(querySize: Int, cuboids: Seq[(Seq[Int], Array[Double])], solver: (Int, Boolean) => WaveletSolver[Double])
}