package core.solver.moment

abstract class MomentTransformer[T](implicit val num: Fractional[T]) {
  //get i0th local moment of a cuboid. Used when we need few moments
  def getMoment(i0: Int, values: Array[T]): T

  val name: String

  //forward transform to moments
  def getMoments(values: Array[T]): Array[T]

  def from1Moment(moments: Array[T]): Array[T]

  def getCoMoments(values: Array[T], pm: Map[Int, T]): Array[T]
  def getCoMoment(i0: Int, values: Array[T], pm: Map[Int, T]): T

  def fromComplementaryMoment(moments: Array[T]) = {
    val result = moments.clone()
    val N = moments.length
    var h = 1
    while (h < N) {
      /* Kronecker product with matrix
         1   0
         1  -1
      */
      (0 until N by h * 2).foreach { i =>
        (i until i + h).foreach { j =>
          result(j + h) = num.minus(result(j), result(j + h))
        }
      }
      h <<= 1
    }
    result
  }

  //reverse transform moments to values
  def getValues(moments: Array[T], handleNegative: Boolean = true): Array[T]
}

case class Moment1Transformer[T: Fractional]() extends MomentTransformer[T] {

  override val name: String = "Moment1"

  override def from1Moment(moments: Array[T]): Array[T] = moments

  override def getMoment(i0: Int, values: Array[T]): T = {
    val rowsToSum = values.indices.filter(i2 => (i2 & i0) == i0)
    rowsToSum.map(values(_)).sum
  }

  override def getValues(moments: Array[T], handleNegative: Boolean): Array[T] = {
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
          val diff = num.minus(result(j), result(j + h))
          if (!handleNegative || (num.gteq(diff, num.zero) && num.gteq(result(j + h), num.zero)))
            result(j) = diff
          else if (num.lt(diff, num.zero)) {
            result(j + h) = result(j)
            result(j) = num.zero
          } else {
            result(j + h) = num.zero
          }
        }
      }
      h *= 2
    }
    result
  }


  override def getCoMoment(i0: Int, values: Array[T], pm: Map[Int, T]): T = ???

  override def getCoMoments(values: Array[T], pm: Map[Int, T]): Array[T] = {
    val result = values.clone()
    val N = values.length
    var h = 1
    /*
    Kronecker product with matrix
        1 1
        -p 1-p
     */
    while (h < N) {
      (0 until N by h * 2).foreach { i =>
        (i until i + h).foreach { j =>
          val first = num.plus(result(j), result(j + h))
          val p = pm(h)
          val second = num.minus(result(j + h), num.times(p, first))
          result(j) = first
          result(j + h) = second
        }
      }
      h *= 2
    }
    result
  }


  override def getMoments(values: Array[T]): Array[T] = {
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
          result(j) = num.plus(result(j), result(j + h))
        }
      }
      h *= 2
    }
    result
  }
}

case class Moment0Transformer[T: Fractional]() extends MomentTransformer[T] {

  override val name: String = "Moment0"

  override def getMoment(i0: Int, values: Array[T]): T = {
    val N = values.length
    val rowsToSum = values.indices.filter(i2 => (i2 & i0) == i0).map(x => N - 1 - x)
    rowsToSum.map(values(_)).sum
  }


  override def getCoMoment(i0: Int, values: Array[T], pm: Map[Int, T]): T = ???

  override def getCoMoments(values: Array[T], pm: Map[Int, T]): Array[T] = ???

  override def from1Moment(moments: Array[T]): Array[T] = fromComplementaryMoment(moments)

  override def getValues(moments: Array[T], handleNegative: Boolean): Array[T] = {
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
          val diff = num.minus(result(j), result(j + h))
          if (!handleNegative || (num.gteq(diff, num.zero) && num.gteq(result(j + h), num.zero))) {
            result(j) = result(j + h)
            result(j + h) = diff
          } else if (num.lt(diff, num.zero)) {
            result(j + h) = num.zero
          } else {
            result(j + h) = result(j)
            result(j) = num.zero
          }
        }
      }
      h *= 2
    }
    result
  }

  override def getMoments(values: Array[T]): Array[T] = {
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
          val sum = num.plus(result(j), result(j + h))
          result(j + h) = result(j)
          result(j) = sum
        }
      }
      h *= 2
    }
    result
  }
}