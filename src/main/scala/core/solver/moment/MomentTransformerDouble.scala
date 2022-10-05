package core.solver.moment


case class Moment1TransformerDouble() extends MomentTransformer[Double] {

  override val name: String = "Moment1"
  type T = Double
  override def from1Moment(moments: Array[T]): Array[T] = moments

  override def getMoment(i0: Int, values: Array[T]): T = {
    val rowsToSum = values.indices.filter(i2 => (i2 & i0) == i0)
    rowsToSum.map(values(_)).sum
  }

  override def getValues(moments: Array[T], handleNegative: Boolean): Array[T] = {
    val result = moments.clone()
    val N = moments.length
    var h = 1
    var i = 0
    var j = 0
    /* Kronecker product with matrix
        1 -1
        0  1
     */
    while (h < N) {
      i = 0
      while (i < N) {
        j = i
        while (j < i + h) {
          val diff = num.minus(result(j), result(j + h))
          if (!handleNegative || (num.gteq(diff, num.zero) && num.gteq(result(j + h), num.zero)))
            result(j) = diff
          else if (num.lt(diff, num.zero)) {
            result(j + h) = result(j)
            result(j) = num.zero
          } else {
            result(j + h) = num.zero
          }
          j += 1
        }
        i += h << 1
      }
      h = h << 1
    }
    result
  }


  override def getCoMoment(i0: Int, values: Array[T], pm: Map[Int, T]): T = ???

  override def getCoMoments(values: Array[T], pm: Map[Int, T]): Array[T] = {
    val result = values.clone()
    val N = values.length
    var h = 1
    var i = 0
    var j = 0
    /*
    Kronecker product with matrix
        1 1
        -p 1-p
     */
    while (h < N) {
      i = 0
      val p = pm(h)
      while (i < N) {
        j = i
        while (j < i + h) {
          val first = num.plus(result(j), result(j + h))
          val second = num.minus(result(j + h), num.times(p, first))
          result(j) = first
          result(j + h) = second
          j += 1
        }
        i += (h << 1)
      }
      h <<= 1
    }
    result
  }


  override def getMoments(values: Array[T]): Array[T] = {
    val result = values.clone()
    val N = values.length
    var h = 1
    var i = 0
    var j = 0
    /*
    Kronecker product with matrix
        1 1
        0 1
     */
    while (h < N) {
      i = 0
      while (i < N) {
        j = 0
        while (j < i + h) {
          result(j) = num.plus(result(j), result(j + h))
          j += 1
        }
        i += (h << 1)
      }
      h <<= 1
    }
    result
  }
}