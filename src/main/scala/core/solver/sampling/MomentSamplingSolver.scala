package core.solver.sampling

import util.BitUtils

import scala.collection.mutable.ListBuffer

class MomentSamplingSolver(val qsize: Int, primaryMoments: Seq[(Int, Double)], version: String, totalSamples: Double) {
  val N = 1 << qsize
  val sampleSolution = Array.fill(N)(0.0)
  var sampleSum = 0.0
  var solution = Array.fill(N)(0.0)
  val momentsToAdd = new ListBuffer[(Int, Double)]()
  val knownSet = collection.mutable.BitSet()
  var pmArray = new Array[Double](qsize)
  var numSamples = 0.0
  val total = primaryMoments.head._2

  if(version != "V3"){
    assert(primaryMoments.head._1 == 0)
    //assert(transformer.isInstanceOf[Moment1Transformer[_]])
    var logh = 0
    primaryMoments.tail.foreach { case (i, m) =>
      assert((1 << logh) == i)
      pmArray(logh) = m / total
      logh += 1
    }
  }

  def addCuboid(eqnColSet: Int, values: Array[Double]): Unit = {
    val colsLength = BitUtils.sizeOfSet(eqnColSet)
    val n0 = 1 << colsLength
    val cols = BitUtils.IntToSet(eqnColSet).reverse.toVector
    val indicesMap = (0 until n0).map(i0 => i0 -> BitUtils.unprojectIntWithInt(i0, eqnColSet))
    val newMomentIndices = indicesMap.filter({ case (i0, i) => !knownSet.contains(i) })

    //need more than log(n0) moments -- do moment transform and filter
    val result = values.clone()
    var logh0 = 0
    var h0 = 1
    var i0 = 0
    var j0 = 0
    /*
    Kronecker product with matrix
        1 1
        -p 1-p
     */
    while (logh0 < colsLength) {
      i0 = 0
      val p = pmArray(cols(logh0))
      while (i0 < n0) {
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
    val cuboid_moments = result
    newMomentIndices.foreach { case (i0, i) =>
      momentsToAdd += i -> cuboid_moments(i0)
      knownSet += i
    }
  }
  def addSample(samples: Array[Long]): Unit = {
    val numSamplesLocal = samples.length >> 1
    (0 until numSamplesLocal).foreach { i =>
      val keyIdx = i << 1
      val valIdx = keyIdx + 1
      val key = samples(keyIdx).toInt
      val value = samples(valIdx).toDouble
      sampleSolution(key) += value
      sampleSum += value
    }
    numSamples += numSamplesLocal
  }

  def  scaleFactor  =  if (numSamples > 0) totalSamples/ numSamples else 0


  def solve() = {

    //First, convert sample to moments with scaling
    val result = sampleSolution.map(x => x * scaleFactor)
    var logh = 0
    var h = 1
    var i = 0
    var j = 0

    if(version != "V3") {
      /*
    Kronecker product with matrix
        1 1
        -p 1-p
     */
      while (logh < qsize) {
        i = 0
        val p = pmArray(logh)
        while (i < N) {
          j = i
          while (j < i + h) {
            val first = result(j) + result(j + h)
            val second = result(j + h) - (p * first)
            result(j) = first
            result(j + h) = second
            j += 1
          }
          i += (h << 1)
        }
        h <<= 1
        logh += 1
      }
    } else {
      //Convert x to m , store the primary moments and then do m to sigma
      while(logh < qsize) {
        i = 0
        while(i < N) {
          j = i
          while(j < i + h) {
            result(j) += result(j + h)
            j += 1
          }
          i += (h << 1)
        }
        h <<= 1
        logh += 1
      }
      logh = 0
      h = 1
      while(logh < qsize) {
        pmArray(logh) = result(h)/result(0)
        h <<= 1
        logh += 1
      }
      h = 1
      logh = 0
      //m to sigma now
      while(logh < qsize) {
        i = 0
        val ph = pmArray(logh)
        while(i < N) {
          j = i
          while(j < i + h) {
            result(j + h) -= ph * result(j)
            j += 1
          }
          i += (h << 1)
        }
        h <<= 1
        logh += 1
      }
    }
    //Next, overwrite known moments
    momentsToAdd.foreach { case (i, m) =>
      //println(s"Moment $i oldvalue = ${result(i)}  newvalue = ${m}")
      assert(version !=  "V3")
      result(i) = m
    }

    //Convert to values
    /*
        Kronecker product with matrix
            1-p -1
            p 1
         */
    h = 1
    logh = 0
    while (h < N) {
      val ph = pmArray(logh)
      var i = 0
      while (i < N) {
        var j = i
        while (j < i + h) {
          result(j + h) = result(j + h) + ph * result(j)
          //result(j) -= result(j + h)
          j += 1
        }
        i += (h << 1)
      }
      h <<= 1
      logh += 1
    }
    h = 1
    var handleNegative = true

    val zero  = 0
    while (h < N) {
      i = 0
      while (i < N) {
        j = i
        while (j < i + h) {
          val diff = result(j) - result(j + h)
          if (!handleNegative || ((diff >= zero) && (result(j + h) >= zero)))
            result(j) = diff
          else if (diff < zero) {
            result(j + h) = result(j)
            result(j) = zero
          } else {
            result(j + h) = zero
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
}
