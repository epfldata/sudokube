package core.solver.moment

import util.{BitUtils, Profiler}

import scala.reflect.ClassTag

class CoMoment5SliceSolver2[T: ClassTag : Fractional](totalsize: Int, slicevalue: IndexedSeq[Int], batchmode: Boolean, transformer: MomentTransformer[T], primaryMoments: Seq[(Int, T)]) extends MomentSolver(totalsize - slicevalue.length, batchmode, transformer, primaryMoments) {
  val solverName = "Comoment5Slice2"
  var pmMap: Map[Int, T] = null
  val sliceMP = collection.mutable.HashMap[Int, T]()

  def getSliceMP(slice0: Int): T = {
    if (sliceMP.isDefinedAt(slice0)) {
      sliceMP(slice0)
    } else {
      var slice = slice0
      var h = N
      var result = num.one
      /* multiply by entry in matrix
        b=0   b=1
sv=0     1-p  -1
sv=1      p    1
*/
      slicevalue.foreach { sv =>
        val b = (slice & 1)
        if (b == 0) {
          val p = pmMap(h)
          if( sv == 0) {
            val oneminusp = num.minus(num.one, p)
            result = num.times(result, oneminusp)
          } else {
            result = num.times(result, p)
          }
        } else {
          if(sv == 0) {
            result = num.negate(result)
          }
        }
        slice >>= 1
        h <<= 1
      }
      sliceMP += slice0 -> result
      result
    }
  }
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

  override def add(eqnColSet: Int, values: Array[T]) {
    val colsLength = BitUtils.sizeOfSet(eqnColSet)
    val n0 = 1 << colsLength

    val newMomentIndices = (0 until n0).map(i0 => i0 -> BitUtils.unprojectIntWithInt(i0, eqnColSet)).
      filter({ case (i0, i) => !knownSet.contains(i) })

    if (false) {
      // need less than log(n0) moments -- find individually
      //TODO: Optimized method to find comoments
    }
    else {
      //need more than log(n0) moments -- do moment transform and filter
      val projectedMomentProduct = (0 until colsLength).map { case b =>
        val i0 = 1 << b
        val i = BitUtils.unprojectIntWithInt(i0, eqnColSet)
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

    val logN = (totalsize - slicevalue.length)
    Profiler("SliceMomentsAdd") {
      momentsToAdd.foreach { case (i, m) =>
        val x = i & (N - 1) //agg index
        val y = i >> logN //slice index
        val p = getSliceMP(y)
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
