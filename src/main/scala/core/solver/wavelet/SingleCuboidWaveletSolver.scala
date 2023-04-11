package core.solver.wavelet

import util.{BitUtils, Profiler}

class SingleCuboidWaveletSolver(override val querySize: Int, val debug: Boolean = false)
  extends
    WaveletSolver("SingleCuboidWaveletSolver", querySize, new HaarTransformer[Double]()) {

  override def solve(): Array[Double] = {
    assert(cuboids.size == 1, "SingleCuboidWaveletSolver can only solve a single cuboid")
    val (variables, cuboid) = cuboids.head

    var wavelet: Array[Double] = null
    Profiler(s"$solverName Forward Transform") {
      // transform cuboid to wavelet
      wavelet = transformer.forward(cuboid)
    }

    var extrapolatedWavelet: Array[Double] = null
    Profiler(s"$solverName Extrapolation") {
      // extrapolate wavelet to the correct size, by filling the missing values with 0
      extrapolatedWavelet = Array.fill(N)(0.0)
      System.arraycopy(wavelet, 0, extrapolatedWavelet, 0, wavelet.length)
    }

    var extrapolatedCuboid: Array[Double] = null
    Profiler(s"$solverName Reverse Transform") {
      // transform wavelet of size N to resulting cuboid
      extrapolatedCuboid = transformer.reverse(extrapolatedWavelet)
    }

    // permute cuboid to the correct position
    val sourceOrder = (0 until querySize).filterNot(i => variables.contains(i)).toList ++ variables
    // target order is ascending bit positions
    val targetOrder = sourceOrder.sorted(Ordering[Int])

    var permutedCuboid: Array[Double] = null
    Profiler(s"$solverName Permutation") {
      val sourceIndex = BitUtils.permute_bits(querySize, sourceOrder.toArray)

      // map values at sourceIndex in extrapolatedCuboid to targetIndex in permutedCuboid
      permutedCuboid = Array.fill(N)(0.0)
      for (i <- 0 until N) {
        permutedCuboid(sourceIndex(i)) = extrapolatedCuboid(i)
      }
    }

    if (debug) {
      println(s"cuboid: $variables ::  ${cuboid.mkString(", ")}")
      println(s"extrapolated wavelet: $sourceOrder ::  ${extrapolatedWavelet.mkString(", ")}")
      println(s"extrapolated cuboid: $sourceOrder ::  ${extrapolatedCuboid.mkString(", ")}")
      println(s"permuted into result: $targetOrder ::  ${permutedCuboid.mkString(", ")}")
      println(s"result as wavelet: $targetOrder ::  ${transformer.forward(permutedCuboid).mkString(", ")}")
    }

    solution = Some((targetOrder, permutedCuboid))
    permutedCuboid
  }
}

