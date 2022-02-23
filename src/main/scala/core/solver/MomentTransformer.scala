package core.solver

abstract class MomentTransformer {
  //get i0th local moment of a cuboid. Used when we need few moments
  def getMoment(i0: Int, values: Array[Double]): Double

  //forward transform to moments
  def getMoments(values: Array[Double]): Array[Double]

  //reverse transform moments to values
  def getValues(moments: Array[Double]): Array[Double]
}

object Moment1Transformer extends MomentTransformer {
  override def getMoment(i0: Int, values: Array[Double]): Double = {
    val rowsToSum = values.indices.filter(i2 => (i2 & i0) == i0)
    rowsToSum.map(values(_)).sum
  }

  override def getValues(moments: Array[Double]): Array[Double] = {
    val result = moments.clone()
    val N = 1 << moments.length
    var h = 1
    while (h < N) {
      /* Kronecker product with matrix
          1 -1
          0  1
       */
      (0 until N by h * 2).foreach { i =>
        (i until i + h).foreach { j =>
          val diff = result(j) - result(j + h)
          if (diff < 0.0 || result(j + h) < 0.0) {
            val half = result(j) / 2.0
            result(j + h) = half
            result(j) = half
          } else
            result(j) = diff
        }
      }
      h *= 2
    }
    result
  }

  override def getMoments(values: Array[Double]): Array[Double] = {
    val result = values.clone()
    val N = 1 << values.length
    var h = 1
    /*
    Kronecker product with matrix
        1 1
        0 1
     */
    while (h < N) {
      (0 until N by h * 2).foreach { i =>
        (i until i + h).foreach { j =>
          result(j) += result(j+h)
        }
      }
      h *= 2
    }
    result
  }
}