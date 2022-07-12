package core.solver.moment

import util.{Bits, Profiler}

import scala.reflect.ClassTag

class CoMoment5Solver[T: ClassTag : Fractional](qsize: Int, batchmode: Boolean, transformer: MomentTransformer[T], primaryMoments: Seq[(Int, T)]) extends MomentSolver(qsize, batchmode, transformer, primaryMoments) {
  override val solverName: String = "Comoment5"
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
    (0 until qsize).foreach { b => knownSet += (1 << b) }

  }

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
      val projectedMomentProduct =  (0 until cols.length).map{ case b =>
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

  def fillMissingBatch(): Unit = {
    Profiler("FillMissingBatch") {
      momentsToAdd.foreach { case (i, m) =>
        val mu = m
        moments(i) = mu
      }
      var h = 1
      while (h < N) {
        (0 until N by h * 2).foreach { i =>
          (i until i + h).foreach { j =>
            moments(j + h) = num.plus(moments(j + h), num.times(pmMap(h), moments(j)))
          }
        }
        h <<= 1
      }
    }
  }


  def fillMissingOnline(): Unit = {

  }
}
