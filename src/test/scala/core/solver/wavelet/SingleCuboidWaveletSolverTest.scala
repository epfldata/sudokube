package core.solver.wavelet

import core.solver.iterativeProportionalFittingSolver.IPFUtils
import org.scalatest.FunSuite
import org.scalatest.Matchers.{convertNumericToPlusOrMinusWrapper, convertToAnyShouldWrapper}
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.prop.TableFor3
import util.BitUtils.SetToInt

import scala.util.Random

class SingleCuboidWaveletSolverTest extends FunSuite {

  val cuboids: TableFor3[Seq[Int], Array[Double], Array[Double]] = Table(
    ("variables", "cuboid", "expected"),
    (Seq(0, 1, 2), Array(1.0, 19.0, 13.0, 1.0, 4.0, 7.0, 1.0, 13.0), Array(1.0, 19.0, 13.0, 1.0, 4.0, 7.0, 1.0, 13.0)),
    (Seq(0, 1), Array(5, 26.0, 14.0, 14.0), Array(2.5, 13.0, 7.0, 7.0, 2.5, 13.0, 7.0, 7.0)),
    (Seq(0, 2), Array(14.0, 20.0, 5.0, 20.0), Array(7.0, 10.0, 7.0, 10.0, 2.5, 10.0, 2.5, 10.0))
  )

  forAll(cuboids) { (variables, cuboid, expected) =>
    val solver = new SingleCuboidWaveletSolver(3)
    solver.addCuboid(variables, cuboid)

    val result = solver.solve()

    assert(result sameElements expected)
  }

  test("zero dim marginal should stay the same") {
    val querySize = Random.nextInt(10) + 1

    // create random array of doubles
    val actual = Array.fill(1 << querySize)(Random.nextDouble())
    val actual_1D_marginal = actual.sum

    // create list of all possible marginals
    // for example, if querySize = 3, then we have 7 marginals (2^3 - 1):
    // Seq(0, 1, 2) -> Seq(0, 1) -> Seq(0, 2) -> Seq(0) -> Seq(1, 2) -> Seq(1) -> Seq(2)
    val marginalIndices = (0 until querySize)
      .toSet
      .subsets()
      .filter(_.nonEmpty)
      .map(_.toSeq.sorted)

    val marginalCuboids = marginalIndices.map {
      marginal => marginal -> IPFUtils.getMarginalDistribution(querySize, actual, marginal.size, SetToInt(marginal))
    }.toMap

    // check for each marginal cuboid
    marginalCuboids.foreach { case (indices, cuboid) =>
      // run through the solver
      val solver = new SingleCuboidWaveletSolver(querySize)
      solver.addCuboid(indices, cuboid)
      val result = solver.solve()

      // check if the one-dimensional marginal is the same as the original
      (result.sum) shouldEqual (actual_1D_marginal +- 0.00000001)
    }
  }
}
