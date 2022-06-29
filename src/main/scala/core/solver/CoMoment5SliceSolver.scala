package core.solver

import util.{Bits, Profiler, Util}

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

class CoMoment5SliceSolver[T: ClassTag : Fractional](totalsize: Int, slicevalue: IndexedSeq[Int], batchmode: Boolean, transformer: MomentTransformer[T], primaryMoments: Seq[(Int, T)]) extends MomentSolver(totalsize, batchmode, transformer, primaryMoments) {
  val solverName = "Comoment5Slice"
  val aggsize = totalsize - slicevalue.length
  val aggN = 1 << aggsize
  var pmMap: Map[Int, T] = null

  override def init(): Unit = {}

  init2()

  def init2(): Unit = {
    moments = Array.fill(N)(num.zero) //using moments array for comoments
    val total = primaryMoments.head._2
    moments(0) = total
    assert(primaryMoments.head._1 == 0)
    assert(transformer.isInstanceOf[Moment1Transformer[_]])
    pmMap = primaryMoments.map { case (i, m) => i -> num.div(m, total) }.toMap

    knownSet += 0
    (0 until totalsize).foreach { b => knownSet += (1 << b) }

  }

  override def solve(hn: Boolean = true) = {
    solution = transformer.getValues(moments.takeRight(aggN), hn)
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
      val projectedMomentProduct = (0 until cols.length).map { case b =>
        val i0 = 1 << b
        val i = Bits.unproject(i0, eqnColSet)
        i0 -> pmMap(i)
      }.toMap + (0 -> pmMap(0))
      val cuboid_moments = transformer.getCoMoments(values, projectedMomentProduct)
      newMomentIndices.foreach { case (i0, i) =>
        momentsToAdd += i -> cuboid_moments(i0)
        knownSet += i
      }
    }
  }

  def fillMissing() = {
    Profiler("MomentsAdd") {
      momentsToAdd.foreach { case (i, m) =>
        moments(i) = m
      }
    }
    var h = N >> 1
    var sliceIdx = if (slicevalue.nonEmpty) slicevalue.indices.last else -1
    var start = 0
    while (h > 0) {
      val name = if (sliceIdx >= 0) "SliceExtrapolate" else "MomentExtrapolate"
      Profiler(name) {
        (start until N by h * 2).foreach { i =>
          (i until i + h).foreach { j =>
            val term = num.plus(moments(j + h), num.times(pmMap(h), moments(j)))
            if (sliceIdx < 0 || slicevalue(sliceIdx) == 1) { //slice 1 or agg
              moments(j + h) = term
            } else { //slice 0
              moments(j + h) = num.minus(moments(j), term)
            }
          }
        }
        if (h >= aggN) start += h
        h >>= 1
        sliceIdx -= 1
      }
    }
  }
}
