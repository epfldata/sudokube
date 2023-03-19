package core.solver.wavelet

class HaarWaveletSolver(val querysize: Int) extends WaveletSolver("HaarWaveletSolver", querysize, new HaarTransformer[Double]()) {

  override def solve(): Array[Double] = {
    val waveletMatrix = Array.fill(N)(0.0)

    cuboids.foreach { case (dimensions, marginalValues) =>
      // transform cuboid to wavelet
      val transformedValues = transformer.forward(marginalValues)

      // (extrapolate) map wavelet to larger wavelet of size N
      transformedValues.indices.foreach { idx =>
        waveletMatrix(idx) += transformedValues(idx)
      }
    }

    // transform wavelet of size N to resulting cuboids
    val result = transformer.reverse(waveletMatrix)

    solution = result
    result
  }
}

