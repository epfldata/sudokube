package core.solver.wavelet

import core.solver.iterativeProportionalFittingSolver.IPFUtils
import org.scalatest.FunSuite
import util.BitUtils.SetToInt

import scala.util.Random

class MutuallyExclusiveWaveletSolverTest extends FunSuite {

  test("direct wavelet permutation should produce same output as cuboid permutation -> transform -> combination") {
    val querySize = Random.nextInt(10) + 1

    // create random array of doubles
    val actual = Array.fill(1 << querySize)(Random.nextDouble())

    // create list of non-overlapping marginals
    // for example, if querySize = 3, then we might have 2 marginals:
    // Seq(0, 2), Seq(1)
    val marginalSize = Random.nextInt(Math.max(querySize / 2, 1)) + 1
    // randomly split (0..querySize) into smaller sets
    val marginalIndices = (0 until querySize).grouped(marginalSize).map(_.sorted.toSeq).toSeq

    println(s"querySize: $querySize, marginalSize: $marginalSize, marginalIndices: ${marginalIndices.toList}")

    val marginalCuboids = marginalIndices.map {
      marginal => marginal -> IPFUtils.getMarginalDistribution(querySize, actual, marginal.size, SetToInt(marginal))
    }.toMap

    // solve with cuboid -> wavelet -> extrapolate + combine
    val meSolver = new MutuallyExclusiveWaveletSolver(querySize)
    marginalCuboids.foreach { case (indices, cuboid) =>
      meSolver.addCuboid(indices, cuboid)
    }
    val meResult = meSolver.solve()

    // solve with cuboid -> wavelet -> extrapolate -> cuboid -> permutate -> wavelet -> combination -> cuboid
    val combinedWavelet = marginalCuboids
      .map { case (indices, cuboid) =>
        val scSolver = new SingleCuboidWaveletSolver(querySize)
        scSolver.addCuboid(indices, cuboid)
        val resultCuboid = scSolver.solve()
        val wavelet = scSolver.transformer.forward(resultCuboid)
        wavelet
      }
      .reduce((a, b) => a.zip(b).map { case (x, y) => x + y })

    // total is added for each cuboid, so we need to divide by the number of cuboids
    combinedWavelet(0) = combinedWavelet(0) / marginalCuboids.size

    val scSolver = new SingleCuboidWaveletSolver(querySize)
    val scResult = scSolver.transformer.reverse(combinedWavelet)

    assert(meResult sameElements scResult)
  }

}
