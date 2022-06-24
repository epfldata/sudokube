package core.solver

import util.{Bits, Profiler}

import scala.reflect.ClassTag

class CoMoment5Solver[T: ClassTag : Fractional](qsize: Int, batchmode: Boolean, transformer: MomentTransformer[T], primaryMoments: Seq[(Int, T)]) extends MomentSolver(qsize, batchmode, transformer, primaryMoments) {
  override val solverName: String = "Comoment5"
  val deltaArray = Array.fill(N)(num.zero)

  override def fillMissing(): Unit = if (batchmode) fillMissingBatch() else fillMissingOnline()


  override def add(cols: Seq[Int], values: Array[T]) {
    val eqnColSet = Bits.toInt(cols)
    val n0 = 1 << cols.length

    val newMomentIndices = (0 until n0).map(i0 => i0 -> Bits.unproject(i0, eqnColSet)).
      filter({ case (i0, i) => !knownSet.contains(i) })

    if (false) {
      // need less than log(n0) moments -- find individually
      //TODO: Optimized method to find comoments
    }
    else {
      //need more than log(n0) moments -- do moment transform and filter
      val projectedMomentProduct = (0 until n0).map{ i0 =>
       val i = Bits.unproject(i0, eqnColSet)
        i0 -> momentProducts(i)
      }.toMap
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
        deltaArray(i) = mu
      }
      var h = 1
      while (h < N) {
        (0 until N by h * 2).foreach { i =>
          (i until i + h).foreach { j =>
              deltaArray(j + h) = num.plus(deltaArray(j + h), num.times(momentProducts(h), deltaArray(j)))
          }
        }
        h *= 2
      }
      moments.indices.foreach { i =>
        moments(i) = num.plus(moments(i), deltaArray(i))
      }
    }
  }


  def fillMissingOnline(): Unit = {

  }
}
