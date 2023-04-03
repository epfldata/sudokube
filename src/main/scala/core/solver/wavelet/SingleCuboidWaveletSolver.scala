package core.solver.wavelet

import util.{BitUtils, Profiler}

class SingleCuboidWaveletSolver(val querysize: Int) extends WaveletSolver("SingleCuboidWaveletSolver", querysize, new HaarTransformer[Double]()) {

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

    var result: Array[Double] = null
    Profiler(s"$solverName Permutation") {
      // permute cuboid to the correct position
      val sourceOrder = variables ++ (0 until querysize).filterNot(i => variables.contains(i))

      // target order is descending bit positions
      val targetOrder = sourceOrder.sorted(Ordering[Int].reverse)

      val sourceIndex = BitUtils.permute_bits(querysize, sourceOrder.toArray)
      val targetIndex = BitUtils.permute_bits(querysize, targetOrder.toArray)

      // map values at sourceIndex in extrapolatedCuboid to targetIndex in result
      result = Array.fill(N)(0.0)
      for (i <- 0 until N) {
        result(targetIndex(i)) = extrapolatedCuboid(sourceIndex(i))
      }
    }


    solution = Some(((0 until N).reverse, result))
    result
  }
}

