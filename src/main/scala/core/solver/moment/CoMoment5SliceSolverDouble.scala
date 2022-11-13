package core.solver.moment

import core.DataCube
import planning.NewProjectionMetaData
import util.{BitUtils, Profiler}

/**
 * @param totalsize Total dimensionality of query including both aggregation and slice
 * @param sliceList List of slice dimensions (index relative to query) along with value. MUST be in increasing order of positions
 * @param batchmode Batch or online mode NOT USED
 * @param transformer NOT USED
 * @param primaryMoments Total sum and 1-D moments in the increasing order of dims
 */
class CoMoment5SliceSolverDouble(totalsize: Int, sliceList: Seq[(Int, Int)], batchmode: Boolean, transformer: MomentTransformer[Double], primaryMoments: Seq[(Int, Double)]) extends MomentSolver[Double](totalsize - sliceList.size, batchmode, transformer, primaryMoments) {
  //transformer is not used
  type T = Double
  val solverName = "Comoment5Slice2"
  var pmArray = new Array[Double](totalsize)
  override def init(): Unit = {}

  init2()

  def init2(): Unit = {
    moments = Array.fill[Double](N)(0) //using moments array for comoments. We only allocate for final result
    val total = primaryMoments.head._2
    assert(primaryMoments.head._1 == 0)
    //assert(transformer.isInstanceOf[Moment1Transformer[_]])
    var logh = 0
    primaryMoments.tail.foreach { case (i, m) =>
      assert((1 << logh) == i)
      pmArray(logh) = m / total
      logh += 1
    }
    // moments(0) is known, but we need it to be present in momentsToAdd

  }

  override def solve(handleNegative: Boolean): Array[Double] = {
    val result: Array[Double] = moments.clone()
    var h = 1
    var i = 0
    var j = 0
    /* Kronecker product with matrix to convert m to x
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
    solution = result
    solution
  }

  def fetchAndAdd(pms: Seq[NewProjectionMetaData], dc: DataCube) = {
    ???
  }

  override def add(eqnColSet: Int, values: Array[Double]) {

    val (colsLength, cols, n0, mn0, sliceMP, sliceDimInCuboid, aggColSet, aggColSetInCuboid) = Profiler("Solve.Add.Init") {
      val colsLength = BitUtils.sizeOfSet(eqnColSet)
      val cols = BitUtils.IntToSet(eqnColSet).reverse.toVector
      var sliceMP = 1.0
      var colsOffsetInCuboid = 0
      var aggcolsOffsetInQuery = 0
      var curDim = 0
      var twoPowercurDim = 1
      var sliceDimInCuboid = List[(Int, Int, Int)]()
      var logm0 = 0
      var aggColSet = 0
      var aggColSetInCuboid = 0

      //Iterate over all dims in eqnColsSet; this list is assumed to be in increasing order of dims
      sliceList.foreach { case (dim, sv) =>
        while (curDim < dim) {
          if ((twoPowercurDim & eqnColSet) != 0) {
            aggColSet |= (1 << aggcolsOffsetInQuery)
            aggColSetInCuboid |= (1 << colsOffsetInCuboid)
            colsOffsetInCuboid += 1
          }
          curDim += 1
          twoPowercurDim <<= 1
          aggcolsOffsetInQuery += 1
        }
        if ((twoPowercurDim & eqnColSet) != 0) { //slice dim in cuboid
          //decreasing order of dims here
          sliceDimInCuboid = (dim, sv, colsOffsetInCuboid) :: sliceDimInCuboid
          logm0 += 1
        } else { //slicedim not in cuboid
          val p = pmArray(dim)
          val pq = if (sv == 1) p else (1 - p)
          sliceMP *= pq
        }

        if ((twoPowercurDim & eqnColSet) != 0) {
          colsOffsetInCuboid += 1
        }
        curDim += 1
        twoPowercurDim <<= 1
      }

      while (twoPowercurDim <= eqnColSet) {
        if ((twoPowercurDim & eqnColSet) != 0) {
          aggColSet |= (1 << aggcolsOffsetInQuery)
          aggColSetInCuboid |= (1 << colsOffsetInCuboid)
          colsOffsetInCuboid += 1
        }
        curDim += 1
        twoPowercurDim <<= 1
        aggcolsOffsetInQuery += 1
      }

      val mn0 = 1 << colsLength
      val logn0 = colsLength - logm0
      val n0 = 1 << logn0

      (colsLength, cols, n0, mn0, sliceMP, sliceDimInCuboid, aggColSet, aggColSetInCuboid)
    }
    val pupIndices = Profiler("Solve.Add.NewMomentIndices.PUP") {
      (0 until mn0).map(i0 => i0 -> BitUtils.unprojectIntWithInt(i0, eqnColSet))
    }
    val (knownIndices, unknownIndices) = Profiler("Solve.Add.NewMomentIndices.Partition") {
      pupIndices.partition { case (i0, i) => knownSet.contains(i) }
    }

    //Both slice and agg dims
    val cuboid_moments = Profiler("Solve.Add.allmoments") {
      val result = values.clone()
      var logh0 = 0
      var h0 = 1
      var i0 = 0
      var j0 = 0
      /*
      Kronecker product with matrix to convert x to mu
          1 1
          -p 1-p
       */
      while (logh0 < colsLength) {
        i0 = 0
        val p = pmArray(cols(logh0))
        while (i0 < mn0) {
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
      result
    }
    Profiler("Solve.Add.ClearKnownMoments") {
      //set known ones to 0 to avoid double counting
      knownIndices.foreach { case (i0, i) => cuboid_moments(i0) = 0 }
    }

    //Convert to conditional moments by applying transformations on slice dims of this cudoid
    //followed by slice dims not in cuboid. The latter is obtained by multiplying every value by sliceMP
    Profiler("Solve.Add.ConvertToConditional") {

      /*
        Kronecker product with matrix
                b=0 b=1
          sv=0  1-p   -1
          sv=1  p   1
         */

      var localsliceCols = 0
      //sliceDimInCuboid is sorted in decreasing order of logh and logh0; we process high dims first before lower
      sliceDimInCuboid.foreach { case (logh, sv, logh0) =>
        val p = pmArray(logh)
        var i0 = 0
        while (i0 < mn0) {
          val h0 = 1 << logh0
          if ((i0 & localsliceCols) == 0) { //only consider those that agree with slice; we move everything to 0-slot.
            var j0 = i0
            while (j0 < i0 + h0) {
              val result = if (sv == 1) {
                cuboid_moments(j0) * p + cuboid_moments(j0 + h0)
              } else
                cuboid_moments(j0) * (1 - p) - cuboid_moments(j0 + h0)
              cuboid_moments(j0) = result //always put in 0-slot
              j0 += 1
            }
          }
          i0 += (h0 << 1)
        }
        localsliceCols |= (1 << logh0)
      }
    }

    Profiler("Solve.Add.addKnown") {
      knownSet ++= unknownIndices.map(_._2)
    }

    Profiler("Solve.Add.addMoments") {
      (0 until n0).map { i0 =>
        val i0agg = BitUtils.unprojectIntWithInt(i0, aggColSetInCuboid)
        val iagg = BitUtils.unprojectIntWithInt(i0, aggColSet)
        val v = cuboid_moments(i0agg)
        if (v != 0.0)
          momentsToAdd += iagg -> (v * sliceMP)
      }
    }
  }


  def fillMissing() = {

    Profiler("SliceMomentsAdd") {
      momentsToAdd.foreach {
        case (i, m) =>
          moments(i) += m
      }
    }

    val aggCols = ((0 until totalsize).toSet.diff(sliceList.map(_._1).toSet)).toVector.sorted
    Profiler("MomentExtrapolate") {
      var h = 1
      var logh = 0
      while (h < N) {
        val p = pmArray(aggCols(logh)) // logh^th aggdim
        var i = 0
        while (i < N) {
          var j = i
          while (j < i + h) {
            moments(j + h) += (p * moments(j))
            j += 1
          }
          i += (h << 1)
        }
        logh += 1
        h <<= 1
      }
    }
  }
}
