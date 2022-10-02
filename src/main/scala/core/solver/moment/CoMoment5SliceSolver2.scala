package core.solver.moment

import core.DataCube
import planning.NewProjectionMetaData
import util.{BitUtils, Profiler}

import scala.reflect.ClassTag

class CoMoment5SliceSolver2[T: ClassTag : Fractional](totalsize: Int, slicevalue: IndexedSeq[Int], batchmode: Boolean, transformer: MomentTransformer[T], primaryMoments: Seq[(Int, T)]) extends MomentSolver(totalsize - slicevalue.length, batchmode, transformer, primaryMoments) {
  val solverName = "Comoment5Slice2"
  var pmMap: Map[Int, T] = null
  val MN = 1 << totalsize
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

      val sliceColSet = eqnColSet >> logN
      val aggColSet = eqnColSet & (N - 1)
      assert((sliceColSet << logN) + aggColSet == eqnColSet)


      val mn0 = 1 << colsLength
      val (logm0, _, projectedSliceIdx) = BitUtils.hwZeroOne(sliceColSet, slicevalue.length)
      val projectedSliceValues = projectedSliceIdx.map(i => slicevalue(i))
      val logn0 = colsLength - logm0
      val n0 = 1 << logn0

      val newMomentIndices = Profiler("NewMomentIndices") {
        (0 until mn0).map(i0 => i0 -> BitUtils.unprojectIntWithInt(i0, eqnColSet)).
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
        val array = Array.fill(mn0)(num.zero)
        newMomentIndices.foreach { case (i0, i) => array(i0) = num.one }



        var h = mn0 >> 1
        //slice part x -> mu -> filter -> x
        projectedSliceValues.foreach { sv =>
          (0 until mn0 by h << 1).foreach { i0 =>
            (i0 until i0 + h).foreach { j0 =>
              val p = projectedMomentProduct(h)
              val oneminusp = num.minus(num.one, p)
              if (sv == 0) {
                /*
                Matrix multiplication
                 1-p  p
                 1-p  p-1
                 */
                val t0 = num.plus(num.times(oneminusp, array(j0)), num.times(p, array(j0 + h)))
                val t1 = num.times(oneminusp, num.minus(array(j0), array(j0 + h)))
                array(j0) = t0
                array(j0 + h) = t1
              } else {
                /*
                 Matrix multiplication
                  p  -p
                  p  1-p
                  */
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

      val aggCoMoments = Array.fill(n0)(num.zero)

      Profiler("MomentSum") {
        array.indices.foreach { i0 =>
          val a0 = i0 & (n0 - 1) //get only the agg bits
          val r = num.times(array(i0), values(i0))
          aggCoMoments(a0) = num.plus(aggCoMoments(a0), r)
        }
      }
      //Do Group by x -> mu transform
      var h = n0 >> 1
      while (h > 0) {
        (0 until n0 by h << 1).foreach { i =>
          (i until i + h).foreach { j =>
            val first = num.plus(aggCoMoments(j), aggCoMoments(j + h))
            val p0 = projectedMomentProduct(h)
            val second = num.minus(aggCoMoments(j + h), num.times(p0, first))
            aggCoMoments(j) = first
            aggCoMoments(j + h) = second
          }
        }
        h >>= 1
      }

      val p = Profiler("getSliceMP") { getSliceMP(sliceColSet) }

      aggCoMoments.indices.foreach { a0 =>
        val a = BitUtils.unprojectIntWithInt(a0, aggColSet)
        momentsToAdd += (a -> num.times(p, aggCoMoments(a0)))
      }

      Profiler("UpdateKnownSet") {
        newMomentIndices.foreach { case (i0, i) =>
          knownSet += i
        }
      }
    }
  }
  override def add(eqnColSet: Int, values: Array[T]) {
    val colsLength = BitUtils.sizeOfSet(eqnColSet)

    val sliceColSet = eqnColSet >> logN  //bitmap for slicecols within the domain of all slicecols
    val aggColSet = eqnColSet & (N - 1)
    assert((sliceColSet << logN) + aggColSet == eqnColSet)

    val mn0 = 1 << colsLength
    val (logm0, _, projectedSliceIdx) = BitUtils.hwZeroOne(sliceColSet,  slicevalue.length)
    val projectedSliceValues = projectedSliceIdx.map(i => slicevalue(i))
    val logn0 = colsLength - logm0
    val n0 = 1 << logn0

    val newMomentIndices = Profiler("NewMomentIndices") {
      (0 until mn0).map(i0 => i0 -> BitUtils.unprojectIntWithInt(i0, eqnColSet)).
        filter({ case (i0, i) => !knownSet.contains(i) })
    }

    val projectedMomentProduct = Profiler("ProjectedMP") {
      (0 until colsLength).map { case b =>
        val i0 = 1 << b
        val i = BitUtils.unprojectIntWithInt(i0, eqnColSet)
        i0 -> pmMap(i)
      }.toMap + (0 -> pmMap(0))
    }

    val array = Array.fill(mn0)(num.zero)
    newMomentIndices.foreach { case (i0, i) => array(i0) = num.one }



    var h = mn0 >> 1
    //slice part x -> mu -> filter -> x
    projectedSliceValues.foreach { sv =>
      (0 until mn0 by h << 1).foreach { i0 =>
        (i0 until i0 + h).foreach { j0 =>
          val p = projectedMomentProduct(h)
          val oneminusp = num.minus(num.one, p)
          if (sv == 0) {
            /*
            Matrix multiplication
             1-p  p
             1-p  p-1
             */
            val t0 = num.plus(num.times(oneminusp, array(j0)), num.times(p, array(j0 + h)))
            val t1 = num.times(oneminusp, num.minus(array(j0), array(j0 + h)))
            array(j0) = t0
            array(j0 + h) = t1
          } else {
            /*
             Matrix multiplication
              p  -p
              p  1-p
              */
            val t0 = num.times(p, num.minus(array(j0), array(j0 + h)))
            val t1 = num.plus(num.times(p, array(j0)), num.times(oneminusp, array(j0 + h)))
            array(j0) = t0
            array(j0 + h) = t1
          }
        }
      }
      h >>= 1
    }

    val aggCoMoments = Array.fill(n0)(num.zero)

    Profiler("MomentSum") {
      array.indices.foreach { i0 =>
        val a0 = i0 & (n0 - 1) //get only the agg bits
        val r = num.times(array(i0), values(i0))
        aggCoMoments(a0) = num.plus(aggCoMoments(a0), r)
      }
    }
    //Do Group by x -> mu transform
    h = n0 >> 1
    while (h > 0) {
      (0 until n0 by h << 1).foreach { i =>
        (i until i + h).foreach { j =>
          val first = num.plus(aggCoMoments(j), aggCoMoments(j + h))
          val p0 = projectedMomentProduct(h)
          val second = num.minus(aggCoMoments(j + h), num.times(p0, first))
          aggCoMoments(j) = first
          aggCoMoments(j + h) = second
        }
      }
      h >>= 1
    }

    val p = Profiler("getSliceMP") { getSliceMP(sliceColSet) }

    aggCoMoments.indices.foreach { a0 =>
      val a = BitUtils.unprojectIntWithInt(a0, aggColSet)
      momentsToAdd += (a -> num.times(p, aggCoMoments(a0)))
    }

    Profiler("UpdateKnownSet") {
      newMomentIndices.foreach { case (i0, i) =>
        knownSet += i
      }
    }
  }


  def fillMissing() = {

    val logN = (totalsize - slicevalue.length)
    Profiler("SliceMomentsAdd") {
      momentsToAdd.foreach {
        case (i, m) =>
          moments(i) = num.plus(moments(i), m)
      }
    }
    Profiler("MomentExtrapolate") {
      var h = 1
      while (h < N) {
        (0 until N by h * 2).foreach {
          i =>
            (i until i + h).foreach {
              j =>
                moments(j + h) = num.plus(moments(j + h), num.times(pmMap(h), moments(j)))
            }
        }
        h <<= 1
      }
    }
  }
}
