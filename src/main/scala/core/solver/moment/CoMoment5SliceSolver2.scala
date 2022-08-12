package core.solver.moment

import core.DataCube
import planning.NewProjectionMetaData
import util.{BitUtils, Profiler}

import scala.reflect.ClassTag

class CoMoment5SliceSolver2[T: ClassTag : Fractional](totalsize: Int, slicevalue: IndexedSeq[Int], batchmode: Boolean, transformer: MomentTransformer[T], primaryMoments: Seq[(Int, T)]) extends MomentSolver(totalsize - slicevalue.length, batchmode, transformer, primaryMoments) {
  val solverName = "Comoment5Slice2"
  var pmMap: Map[Int, T] = null
  assert(totalsize == slicevalue.length)
  val sliceMP = collection.mutable.HashMap[Int, T]()

  def getSliceMP(slice0: Int): T = {
    if (sliceMP.isDefinedAt(slice0)) {
      sliceMP(slice0)
    } else {
      var slice = slice0
      var h = N
      var result = num.one
      /* multiply by entry in matrix for each b=0 ONLY
        b=0   b=1
sv=0     1-p  -1
sv=1      p    1
*/
      slicevalue.foreach { sv =>
        val b = (slice & 1)
        if (b == 0) {
          val p = pmMap(h)
          if (sv == 0) {
            val oneminusp = num.minus(num.one, p)
            result = num.times(result, oneminusp)
          } else {
            result = num.times(result, p)
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
    //(0 until totalsize).foreach { b => knownSet += (1 << b) }
  }

  override def solve(hn: Boolean = true) = {
    solution = transformer.getValues(moments, hn)
    solution
  }

  def fetchAndAdd(pms: Seq[NewProjectionMetaData], dc: DataCube) = {
    pms.foreach { pm =>
      val eqnColSet = pm.queryIntersection
      val colsLength = pm.queryIntersectionSize
      val n0 = 1 << colsLength


      val newMomentIndices = Profiler("NewMomentIndices") {
        (0 until n0).map(i0 => i0 -> BitUtils.unprojectIntWithInt(i0, eqnColSet)).
          filter({ case (i0, i) => !knownSet.contains(i) })
      }

      val projectedMomentProduct = Profiler("ProjectedMP") {
        (0 until colsLength).map { case b =>
          val i0 = 1 << b
          val i = BitUtils.unprojectIntWithInt(i0, eqnColSet)
          i0 -> pmMap(i)
        }.toMap + (0 -> pmMap(0))
      }

      val (array, maskArray) = Profiler("MaskArrayComputation") {
        val array = Array.fill(n0)(num.zero)
        newMomentIndices.foreach { case (i0, i) => array(i0) = num.one }

        val projectedSliceColSet = BitUtils.IntToSet(eqnColSet >> logN) //MSB to LSB //TODO: FIXME assumes that the slice cols are going to be top bits in the projections too.
        val aggn0 = n0 >> projectedSliceColSet.length
        val projectedSliceValues = projectedSliceColSet.map(i => slicevalue(i))

        var h = n0 >> 1
        projectedSliceValues.foreach { sv =>
          (0 until n0 by h << 1).foreach { i0 =>
            (i0 until i0 + h).foreach { j0 =>
              val p = projectedMomentProduct(h)
              val oneminusp = num.minus(num.one, p)
              if (sv == 0) {
                val t0 = num.plus(num.times(oneminusp, array(j0)), num.times(p, array(j0 + h)))
                val t1 = num.times(oneminusp, num.minus(array(j0), array(j0 + h)))
                array(j0) = t0
                array(j0 + h) = t1
              } else {
                val t0 = num.times(p, num.minus(array(j0), array(j0 + h)))
                val t1 = num.plus(num.times(p, array(j0)), num.times(oneminusp, array(j0 + h)))
                array(j0) = t0
                array(j0 + h) = t1
              }
            }
          }
          h >>= 1
        }
        val maskArray = array.map { (_ != num.zero) }
        (array, maskArray)
      }
      val values = Profiler("FetchSlice") { dc.fetchWithSliceMask(pm, maskArray) }

      val newMoment = Profiler("MomentSum"){ array.indices.map(i => num.times(array(i), values(i))).sum }
      val p = Profiler("getSliceMP") { getSliceMP(eqnColSet) }
      momentsToAdd += (0 -> num.times(p, newMoment))
      Profiler("UpdateKnownSet") {
        newMomentIndices.foreach { case (i0, i) =>
          //  momentsToAdd += i -> cuboid_moments(i0)
          knownSet += i
        }
      }
    }
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

      val array = Array.fill(n0)(num.zero)
      newMomentIndices.foreach { case (i0, i) => array(i0) = num.one }

      val projectedSliceColSet = BitUtils.IntToSet(eqnColSet >> logN) //MSB to LSB //TODO: FIXME assumes that the slice cols are going to be top bits in the projections too.
      val aggn0 = n0 >> projectedSliceColSet.length
      val projectedSliceValues = projectedSliceColSet.map(i => slicevalue(i)) //projected and in reverse order from MSB to LSB

      var h = n0 >> 1
      projectedSliceValues.foreach { sv =>
        (0 until n0 by h << 1).foreach { i0 =>
          (i0 until i0 + h).foreach { j0 =>
            val p = projectedMomentProduct(h)
            val oneminusp = num.minus(num.one, p)
            if (sv == 0) {
              val t0 = num.plus(num.times(oneminusp, array(j0)), num.times(p, array(j0 + h)))
              val t1 = num.times(oneminusp, num.minus(array(j0), array(j0 + h)))
              array(j0) = t0
              array(j0 + h) = t1
            } else {
              val t0 = num.times(p, num.minus(array(j0), array(j0 + h)))
              val t1 = num.plus(num.times(p, array(j0)), num.times(oneminusp, array(j0 + h)))
              array(j0) = t0
              array(j0 + h) = t1
            }
          }
        }
        h >>= 1
      }
      //TODO: groupby aggregation
      val newMoment = array.indices.map(i => num.times(array(i), values(i))).sum
      val p = getSliceMP(eqnColSet)
      momentsToAdd += (0 -> num.times(p, newMoment))
      newMomentIndices.foreach { case (i0, i) =>
        //  momentsToAdd += i -> cuboid_moments(i0)
        knownSet += i
      }
    }
  }

  def fillMissing() = {

    val logN = (totalsize - slicevalue.length)
    Profiler("SliceMomentsAdd") {
      momentsToAdd.foreach { case (i, m) =>
        moments(i) = num.plus(moments(i), m)
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
