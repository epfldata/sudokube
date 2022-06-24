package core.solver

import util.Bits

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

class CoMoment5SliceSolver[T: ClassTag : Fractional](totalsize: Int, slicevalue: IndexedSeq[Int], batchmode: Boolean, transformer: MomentTransformer[T], primaryMoments: Seq[(Int, T)]) extends MomentSolver(totalsize , batchmode, transformer, primaryMoments) {
  val solverName = "Comoment5Slice"
  val aggsize = totalsize - slicevalue.length
  val aggN = 1 << aggsize
  val deltaArray = Array.fill(N)(num.zero)
  solution = transformer.getValues(moments.takeRight(aggN))

  override def solve() = {
    solution = transformer.getValues(moments.takeRight(aggN))
    solution
  }

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
      val projectedMomentProduct = (0 until n0).map { i0 =>
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

  def fillMissing() = {
    momentsToAdd.foreach { case (i, m) =>
      val mu = m
      deltaArray(i) = mu
    }

    var h = N >> 1
    var start = 0
    while (h > 0) {
      (start until N by h * 2).foreach { i =>
        (i until i + h).foreach { j =>
          deltaArray(j + h) = num.plus(deltaArray(j + h), num.times(momentProducts(h), deltaArray(j)))
        }
      }
      if (h >= aggN) start += h
      h >>= 1
    }
    moments.indices.takeRight(aggN).foreach { i =>
      moments(i) = num.plus(moments(i), deltaArray(i))
    }
  }

}
