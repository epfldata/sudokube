package core.solver.wavelet

import util.{BitUtils, Profiler}

class MutuallyExclusiveWaveletSolver(override val querySize: Int,
                                     override val debug: Boolean = false)
  extends
    WaveletSolver("MutuallyExclusiveWaveletSolver", querySize, new HaarTransformer[Double](), debug) {


  override def solve(): Array[Double] = {
    // check if cuboids are mutually exclusive
    val cuboidVariables = cuboids.map(_._1)
    assert(cuboidVariables.map(_.size).sum == cuboidVariables.flatten.toSet.size, "Cuboids are not mutually exclusive")

    var finalWavelet: Array[Double] = Array.empty

    // target order is ascending bit positions
    val targetOrder = (0 until querySize).sorted(Ordering[Int])

    for (((variables, cuboid), index) <- cuboids.zipWithIndex) {

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

      var extrapolatedCuboid: Array[Double] = null
      Profiler(s"$solverName Reverse Transform") {
        // transform wavelet of size N to resulting cuboid
        extrapolatedCuboid = transformer.reverse(extrapolatedWavelet)
      }

      // permute cuboid to the correct position
      val sourceOrder = (0 until querySize).filterNot(i => variables.contains(i)).toList ++ variables

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

      val permutedWavelet: Array[Double] = Profiler(s"$solverName Forward Transform") {
        transformer.forward(permutedCuboid)
      }

      Profiler(s"$solverName Combination") {
        // add permuted wavelet to result wavelet
        if (index == 0)
          finalWavelet = permutedWavelet
        else {
          // given source wavelet   (1)       :: [total, x, 1_0, 1_1,   x,   x,   x,   x]
          // given source wavelet   (0, 2)    :: [total, 2,   x,   x, 0_0, 0_1, 0_2, 0_3]
          // we need to get target  (0, 1, 2) :: [total, 2, 1_0, 1_1, 0_0, 0_1, 0_2, 0_3]

          // since the unknowns are filled with zero's we can just add the wavelets
          for (i <- 1 until N)
            finalWavelet(i) += permutedWavelet(i)
        }
      }

      if (debug) {
        println(s"Source Cuboid: $sourceOrder :: ${cuboid.mkString(", ")}")
        println(s"Into Wavelet:  $sourceOrder :: ${extrapolatedWavelet.mkString(", ")}")
        println(s"Permuted Wavelet: $targetOrder :: ${permutedWavelet.mkString(", ")}")
        println()
      }
    }

    val finalCuboid: Array[Double] = transformer.reverse(finalWavelet)

    if (debug) {
      println(s"Final Wavelet: $targetOrder :: ${finalWavelet.mkString(", ")}")
      println(s"Final Cuboid: $targetOrder :: ${finalCuboid.mkString(", ")}")
      println()
    }

    solution = Some((targetOrder, finalCuboid))
    finalCuboid
  }
}
