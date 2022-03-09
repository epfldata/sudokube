package core.solver

abstract class MomentTransformer {
  //get i0th local moment of a cuboid. Used when we need few moments
  def getMoment(i0: Int, values: Array[Double]): Double
  val name: String
  //forward transform to moments
  def getMoments(values: Array[Double]): Array[Double]

  //reverse transform moments to values
  def getValues(moments: Array[Double], handleNegative: Boolean = true): Array[Double]
}

object Moment1Transformer extends MomentTransformer {

  override val name: String = "Moment1"

  override def getMoment(i0: Int, values: Array[Double]): Double = {
    val rowsToSum = values.indices.filter(i2 => (i2 & i0) == i0)
    rowsToSum.map(values(_)).sum
  }

  override def getValues(moments: Array[Double], handleNegative: Boolean): Array[Double] = {
    val result = moments.clone()
    val N = moments.length
    var h = 1
    while (h < N) {
      /* Kronecker product with matrix
          1 -1
          0  1
       */
      (0 until N by h * 2).foreach { i =>
        (i until i + h).foreach { j =>
          val diff = result(j) - result(j + h)
          if (!handleNegative || (diff >= 0 && result(j + h) >= 0))
            result(j) = diff
          else if (diff < 0) {
            result(j + h) = result(j)
            result(j) = 0
          } else {
            result(j + h) = 0
          }
        }
      }
      h *= 2
    }
    result
  }

  override def getMoments(values: Array[Double]): Array[Double] = {
    val result = values.clone()
    val N = values.length
    var h = 1
    /*
    Kronecker product with matrix
        1 1
        0 1
     */
    while (h < N) {
      (0 until N by h * 2).foreach { i =>
        (i until i + h).foreach { j =>
          result(j) += result(j + h)
        }
      }
      h *= 2
    }
    result
  }
}

object Moment0Transformer extends MomentTransformer {

  override val name: String = "Moment0"

  override def getMoment(i0: Int, values: Array[Double]): Double = {
    val N = values.length
    val rowsToSum = values.indices.filter(i2 => (i2 & i0) == i0).map(x => N-1-x)
    rowsToSum.map(values(_)).sum
  }

  override def getValues(moments: Array[Double], handleNegative: Boolean): Array[Double] = {
    val result = moments.clone()
    val N = moments.length
    var h = 1
    while (h < N) {
      /* Kronecker product with matrix
          0 1
          1 -1
       */
      (0 until N by h * 2).foreach { i =>
        (i until i + h).foreach { j =>
          val diff = result(j) - result(j + h)
          if (!handleNegative || (diff >= 0 && result(j + h) >= 0)) {
            result(j) = result(j + h)
            result(j + h) = diff
          } else if (diff < 0) {
            result(j + h) = 0
          } else {
            result(j + h) = result(j)
            result(j) = 0
          }
        }
      }
      h *= 2
    }
    result
  }

  override def getMoments(values: Array[Double]): Array[Double] = {
    val result = values.clone()
    val N = values.length
    var h = 1
    /*
    Kronecker product with matrix
        1 1
        1 0
     */
    while (h < N) {
      (0 until N by h * 2).foreach { i =>
        (i until i + h).foreach { j =>
          val sum = result(j) + result(j + h)
          result(j + h) = result(j)
          result(j) = sum
        }
      }
      h *= 2
    }
    result
  }
}