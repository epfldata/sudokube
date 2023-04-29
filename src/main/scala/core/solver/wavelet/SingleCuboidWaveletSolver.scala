package core.solver.wavelet

import util.{BitUtils, Profiler}

class SingleCuboidWaveletSolver(override val querySize: Int,
                                override val debug: Boolean = false)
  extends
    WaveletSolver[Double]("SingleCuboidWaveletSolver", querySize, debug) {

  override def solve(): Array[Double] = {
    assert(cuboids.size == 1, "SingleCuboidWaveletSolver can only solve a single cuboid")
    val (variables, cuboid) = cuboids.head

    val wavelet: Array[Double] = Profiler(s"$solverName Forward Transform") {
      // transform cuboid to wavelet
      transformer.forward(cuboid)
    }

    val extrapolatedWavelet: Array[Double] = Profiler(s"$solverName Extrapolation") {
      // extrapolate wavelet to the correct size, by filling the missing values with 0
      val extrapolatedWavelet = Array.fill(N)(0.0)
      System.arraycopy(wavelet, 0, extrapolatedWavelet, 0, wavelet.length)

      extrapolatedWavelet
    }

    val extrapolatedCuboid: Array[Double] = Profiler(s"$solverName Reverse Transform") {
      // transform wavelet of size N to resulting cuboid
      transformer.reverse(extrapolatedWavelet)
    }

    // permute cuboid to the correct position
    val sourceOrder = (0 until querySize).filterNot(i => variables.contains(i)).toList ++ variables
    // target order is ascending bit positions
    val targetOrder = sourceOrder.sorted(Ordering[Int])

    val permutedCuboid: Array[Double] = Profiler(s"$solverName Permutation") {
      val permuteIndex = BitUtils.permute_bits(querySize, sourceOrder.toArray)

      // map values at sourceIndex in extrapolatedCuboid to targetIndex in permutedCuboid
      val permutedCuboid = Array.fill(N)(0.0)
      for (i <- 0 until N) {
        val targetIndex = permuteIndex(i)
        permutedCuboid(targetIndex) = extrapolatedCuboid(i)
      }

      permutedCuboid
    }

    if (debug) {
      println(s"cuboid: $variables ::  ${cuboid.mkString(", ")}")
      println(s"extrapolated wavelet: $sourceOrder ::  ${extrapolatedWavelet.mkString(", ")}")
      println(s"extrapolated cuboid: $sourceOrder ::  ${extrapolatedCuboid.mkString(", ")}")
      println(s"permuted into result: $targetOrder ::  ${permutedCuboid.mkString(", ")}")
      println(s"result as wavelet: $targetOrder ::  ${transformer.forward(permutedCuboid).mkString(", ")}")
    }

    solution = permutedCuboid
    permutedCuboid
  }
}

