package core.solver.moment

import util.{BitUtils, Profiler}

import scala.reflect.ClassTag

class CoMoment5Solver[T: ClassTag : Fractional](qsize: Int, batchmode: Boolean, transformer: MomentTransformer[T], primaryMoments: Seq[(Int, T)]) extends MomentSolver(qsize, batchmode, transformer, primaryMoments) {
  override val solverName: String = "Comoment5"
  val pmArray = new Array[T](qsize)

  override def init(): Unit = {}

  init2()
  def init2(): Unit = {
    moments = Array.fill(N)(num.zero) //using moments array for comoments
    val total = primaryMoments.head._2
    moments(0) = total
    assert(primaryMoments.head._1 == 0)
    assert(transformer.isInstanceOf[Moment1Transformer[_]])
    var logh = 0
    primaryMoments.tail.foreach { case (i, m) =>
      assert((1 << logh) == i)
      pmArray(logh) = num.div(m, total)
      logh += 1
    }
    knownSet += 0
    (0 until qsize).foreach { b => knownSet += (1 << b) }

  }

  override def fillMissing(): Unit = if (batchmode) fillMissingBatch() else fillMissingOnline()


  override def add(eqnColSet: Int, values: Array[T]): Unit = {
    val colsLength = BitUtils.sizeOfSet(eqnColSet)
    val cols = BitUtils.IntToSet(eqnColSet).reverse.toVector
    val n0 = 1 << colsLength

    val newMomentIndices = (0 until n0).map(i0 => i0 -> BitUtils.unprojectIntWithInt(i0, eqnColSet)).
      filter({ case (i0, i) => !knownSet.contains(i) })

    if (false) {
      // need less than log(n0) moments -- find individually
      //TODO: Optimized method to find comoments
    }
    else {
      //need more than log(n0) moments -- do moment transform and filter

      val projectedMomentProduct = new Array[T](colsLength)
      (0 until colsLength).foreach { case b =>
        projectedMomentProduct(b) = pmArray(cols(b))
      }
      val cuboid_moments = transformer.getCoMoments(values, projectedMomentProduct)
      newMomentIndices.foreach { case (i0, i) =>
        momentsToAdd += i -> cuboid_moments(i0)
        knownSet += i
      }
    }
  }

  def fillMissingBatch(): Unit = {
    Profiler("FillMissingBatch") {
      momentsToAdd.foreach { case (i, m) =>
        val mu = m
        moments(i) = mu
      }
      var h = 1
      var logh = 0
      var i = 0
      var j = 0
      while (h < N) {
        i = 0
        val ph = pmArray(logh)
        while (i < N) {
          j = i
          while (j < i + h) {
            moments(j + h) = num.plus(moments(j + h), num.times(ph, moments(j)))
            j += 1
          }
          i += (h << 1)
        }
        h <<= 1
        logh += 1
      }
    }
  }


  def fillMissingOnline(): Unit = {

  }
}
