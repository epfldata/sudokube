package core.solver.moment

import util.BitUtils
class CoMoment5SolverDouble(qsize: Int, batchmode: Boolean, transformer: MomentTransformer[Double], primaryMoments: Seq[(Int, Double)]) extends MomentSolver[Double](qsize, batchmode, transformer, primaryMoments) {
  override val solverName: String = "Comoment5"
  val pmArray = new Array[Double](qsize)
  override def init(): Unit = {}
  init2()
  def init2(): Unit = {
    moments = Array.fill[Double](N)(0) //using moments array for comoments
    val total = primaryMoments.head._2
    moments(0) = total
    assert(primaryMoments.head._1 == 0)
    var logh = 0
    primaryMoments.tail.foreach { case (i, m) =>
      assert((1 << logh) == i)
      pmArray(logh) = m / total
      logh += 1
    }

    knownSet += 0
    (0 until qsize).foreach { b => knownSet += (1 << b) }

  }

  override def fillMissing(): Unit = if (batchmode) fillMissingBatch() else fillMissingOnline()

  override def add(eqnColSet: Int, values: Array[Double]): Unit = {
    val colsLength = BitUtils.sizeOfSet(eqnColSet)
    val n0 = 1 << colsLength
    val cols = BitUtils.IntToSet(eqnColSet).reverse.toVector
    val newMomentIndices = (0 until n0).map(i0 => i0 -> BitUtils.unprojectIntWithInt(i0, eqnColSet)).
      filter({ case (i0, i) => !knownSet.contains(i) })

    if (false) {
      // need less than log(n0) moments -- find individually
      //TODO: Optimized method to find comoments
    }
    else {
      //need more than log(n0) moments -- do moment transform and filter
      val result = values.clone()
      var logh0 = 0
      var h0 = 1
      var i0 = 0
      var j0 = 0
      /*
      Kronecker product with matrix
          1 1
          -p 1-p
       */
      while (logh0 < colsLength) {
        i0 = 0
        val p = pmArray(cols(logh0))
        while (i0 < n0) {
          j0 = i0
          while (j0 < i0 + h0) {
            val first = result(j0) + result(j0 + h0)
            val second = result(j0 + h0) - (p * first)
            result(j0) = first
            result(j0 + h0) = second
            j0 += 1
          }
          i0 += (h0 << 1)
        }
        h0 <<= 1
        logh0 += 1
      }
      val cuboid_moments = result
      newMomentIndices.foreach { case (i0, i) =>
        momentsToAdd += i -> cuboid_moments(i0)
        knownSet += i
      }
    }
  }

  def fillMissingBatch(): Unit = {
    momentsToAdd.foreach { case (i, m) =>
      val mu = m
      moments(i) = mu
    }
    var h = 1
    var logh = 0
    while (h < N) {
      val ph = pmArray(logh)
      (0 until N by h << 1).foreach { i =>
        (i until i + h).foreach { j =>
          moments(j + h) = moments(j + h) + ph * moments(j)
        }
      }
      h <<= 1
      logh += 1
    }
  }

  def fillMissingOnline(): Unit = {

  }

  override def solve(handleNegative: Boolean): Array[Double] = {
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
          val diff = result(j) - result(j + h)
          if (!handleNegative || ((diff >= 0) && (result(j + h) >= 0)))
            result(j) = diff
          else if (diff < 0) {
            result(j + h) = result(j)
            result(j) = 0
          } else {
            result(j + h) = 0
          }
          j += 1
        }
        i += h << 1
      }
      h = h << 1
    }
    solution = result
    solution
  }
}
