package core.solver.moment

import util.{Bits, Profiler}

import scala.reflect.ClassTag

class CoMoment5SliceSolver2[T: ClassTag : Fractional](totalsize: Int, slicevalue: IndexedSeq[Int], batchmode: Boolean, transformer: MomentTransformer[T], primaryMoments: Seq[(Int, T)]) extends MomentSolver(totalsize - slicevalue.length, batchmode, transformer, primaryMoments) {
  val solverName = "Comoment5Slice2"
  var pmMap: Map[Int, T] = null

  override def init(): Unit = {}

  init2()

  def init2(): Unit = {
    moments = Array.fill(N)(num.zero) //using moments array for comoments. We only allocate for final result
    val total = primaryMoments.head._2
    assert(primaryMoments.head._1 == 0)
    assert(transformer.isInstanceOf[Moment1Transformer[_]])
    pmMap = primaryMoments.map { case (i, m) => i -> num.div(m, total) }.toMap

    // moments(0) is known, but we need it to be present in momentsToAdd
    //knownSet += 0
    (0 until totalsize).foreach { b => knownSet += (1 << b) }
  }

  override def solve(hn: Boolean = true) = {
    solution = transformer.getValues(moments, hn)
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

    val M = 1 << slicevalue.length
    val sliceMomentProduct = Array.fill[T](M)(num.one)
    Profiler("BuildSliceProduct") {
      var h = 1
      var h2 = N
      slicevalue.foreach { sv => //while (h < M)
        /* multiply by entry in matrix
                j   j + h
       sv=0     1-p  -1
       sv=1      p    1
       */
        val p = pmMap(h2)
        val oneminusp = num.minus(num.one, p)
        if (sv == 0) {
          (0 until M by h * 2).foreach { i =>
            (i until i + h).foreach { j =>
              sliceMomentProduct(j) = num.times(sliceMomentProduct(j), oneminusp)
              sliceMomentProduct(j + h) = num.negate(sliceMomentProduct(j + h))
            }
          }
        } else {
          (0 until M by h * 2).foreach { i =>
            (i until i + h).foreach { j =>
              sliceMomentProduct(j) = num.times(sliceMomentProduct(j), p)
            }
          }
        }
        h <<= 1
        h2 <<= 1
      }
    }
    val logN = (totalsize - slicevalue.length)
    Profiler("SliceMomentsAdd") {
      momentsToAdd.foreach { case (i, m) =>
        val x = i & (N - 1) //agg index
        val y = i >> logN //slice index
        val p = sliceMomentProduct(y)
        moments(x) = num.plus(moments(x), num.times(p, m))
      }
    }
    Profiler("MomentExtrapolate") {
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
}
