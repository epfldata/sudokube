package core.solver.moment

import util.BitUtils

class CoMoment5SolverDouble(qsize: Int, batchmode: Boolean, transformer: MomentTransformer[Double], primaryMoments: Seq[(Int, Double)], handleZeroes: Boolean = false, secondOrder: Boolean = false) extends MomentSolver[Double](qsize, batchmode, transformer, primaryMoments) {
  override val solverName: String = "Comoment5"
  var cutOffZero = false
  var cutOffZeroCount = 0.0
  var cutOffZeroValueSum = 0.0
  val pmArray = new Array[Double](qsize)
  val zeros = collection.mutable.BitSet()
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
    val indicesMap = (0 until n0).map(i0 => i0 -> BitUtils.unprojectIntWithInt(i0, eqnColSet))
    val newMomentIndices = indicesMap.filter({ case (i0, i) => !knownSet.contains(i) })

    if (handleZeroes) {
      val localZeroes = values.indices.filter(i => values(i) == 0.0)
      val nonEqnColSet = (N - 1) - eqnColSet
      val projectedEntries = (0 until 1 << (qsize - colsLength)).map(k0 => BitUtils.unprojectIntWithInt(k0, nonEqnColSet))
      localZeroes.foreach { i0 =>
        val i = indicesMap(i0)._2
        zeros ++= projectedEntries.map { k => i + k }
      }
    }
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
    momentsToAdd.foreach {
      case (i, m) =>
        val mu = m
        moments(i) = mu
    }
    if (secondOrder) {
      val total = moments(0)
      val sqrtN = (2 << (qsize >> 1)) //2 times sqrtN
      val sortedMoments = moments.zipWithIndex.sortBy { case (mu, i) => -math.abs(mu) }
      val topSortedMoments = sortedMoments.take(sqrtN)
      val processed = collection.mutable.BitSet()
      topSortedMoments.foreach { case (mu1, i1) =>
        topSortedMoments.foreach { case (mu2, i2) =>
          if (((i1 & i2) == 0) && (i1 < i2)) {
            val i = i1 + i2
            processed += i
            if (!knownSet(i)) {
              assert(processed(i) || moments(i) == 0.0, s"Nonzero initial moment for $i")
              moments(i) += (mu1 * mu2) / total
            }
          }
        }
      }
    }
    var h = 1
    var logh = 0
    while (h < N) {
      val ph = pmArray(logh)
      var i = 0
      while (i < N) {
        var j = i
        while (j < i + h) {
          moments(j + h) = moments(j + h) + ph * moments(j)
          j += 1
        }
        i += (h << 1)
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
    if (cutOffZero) {
      cutOffZeroCount = 0.0
      cutOffZeroValueSum = 0.0
      assert(!handleNegative)
      i = 0
      while (i < N) {
        if (result(i) < 0) {
          cutOffZeroCount += 1
          cutOffZeroValueSum -= result(i)
          result(i) = 0
        }
        i += 1
      }
    }
    if (handleZeroes) {
      result.indices.filter(i => zeros(i)).foreach {
        i => result(i) = 0.0
      }
    }

    solution = result
    solution
  }
}
