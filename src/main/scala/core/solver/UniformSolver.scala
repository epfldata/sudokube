package core.solver

import breeze.linalg.{DenseMatrix, DenseVector, inv, scale, strictlyUpperTriangular}
import combinatorics.Combinatorics
import util.{BigBinary, Bits, Profiler, Util}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class UniformSolver[T: ClassTag](val qsize: Int)(implicit num: Fractional[T]) {
  var allowNegative = false
  var setSimpleDefault = false
  val N = 1 << qsize
  assert(qsize < 31)
  val hamming_order = (0 until N).sortBy(i => BigBinary(i).hamming_weight)
  val sumValues = Util.mkAB(N, _ => num.zero)
  val knownSums = collection.mutable.BitSet()

  var fetchedCuboids = List[(Int, Array[Double])]()
  var solution: Array[Double] = null

  def verifySolution() = {
    //solution.indices.foreach(i => if (solution(i) < 0) println(s"solution[$i] = ${solution(i)}"))
    //assert(solution.map(_ >= 0).reduce(_ && _))

    fetchedCuboids.foreach {
      case (cols, values) =>
        //println("Checking cuboid " + cols)
        val projection = (0 until solution.length).groupBy(i => Bits.project(i, cols)).mapValues {
          idxes => idxes.map(solution(_)).sum
        }.toSeq.sortBy(_._1).map(_._2)
        values.zip(projection).zipWithIndex.foreach { case ((v, p), i) => if (Math.abs(v - p) > 0.0001) println(s"$i :: $v != $p") }
        assert(values.zip(projection).map { case (v, p) => Math.abs(v - p) <= 0.0001 }.reduce(_ && _))
    }
  }

  def dof = N - knownSums.size

  def errMax = {
    val delta = new Array[T](N)
    hamming_order.foreach{ row =>
      if(knownSums(row))
        delta(row) = num.zero
      else {
        val n = BigBinary(row).hamming_weight
        val lb = lowerBound(row)
        val ub = upperBound(row)
        delta(row) = num.div(num.plus(lb, ub), num.fromInt(2))
      }
    }
    var h = 1
    while (h < N) {
      (0 until N by h * 2).foreach { i =>
        (i until i + h).foreach { j =>
          delta(j) = num.max(delta(j), delta(j + h))
        }
      }
      h *= 2
    }
    num.toDouble(num.div(delta.sum,sumValues(0)))
  }

  def lowerBound(row: Int) = {
    val n = BigBinary(row).hamming_weight
    val combSum = Combinatorics.mk_comb_bi(n, n - 1).map(i => Bits.unproject(i.toInt, row)).map(sumValues(_)).sum
    num.max(num.zero, num.plus(num.times(num.fromInt(1 - n), sumValues(0)), combSum))
  }
  def upperBound(row: Int) = {
    val n = BigBinary(row).hamming_weight
    val combMin = Combinatorics.mk_comb_bi(n, n - 1).map(i => Bits.unproject(i.toInt, row)).map(sumValues(_)).min
    combMin
  }
  def setDefaultValue(row: Int) = {
    if (setSimpleDefault) {
      val lb = lowerBound(row)
      val ub = upperBound(row)
      //println(s"Bounds for $row = ($lb, $ub)")
      sumValues(row) =  num.div(num.plus(lb, ub), num.fromInt(2))
    } else {
      val n = BigBinary(row).hamming_weight

      def getMeanProduct(colSet: Int) =
        Bits.fromInt(colSet).map { c =>
          num.div(sumValues(1 << c), sumValues(0))
        }.product

      //Special case for means
      val sum = if (n == 1) num.div(sumValues(0), num.fromInt(2)) else (1 to n).map { k =>

        //WARNING: Converting BigInt to Int. Ensure that query does not involve more than 30 bits
        val combs = Combinatorics.mk_comb_bi(n, k).map(i => Bits.unproject(i.toInt, row))
        val sign = if ((k % 2) == 1) num.one else num.negate(num.one)
        //TODO: Can simplify further if parents were unknown and default values were used for them
        num.times(sign, combs.map { i =>
          num.times(sumValues(row - i), getMeanProduct(i))
        }.sum)
      }.sum
      val combMin = Combinatorics.mk_comb_bi(n, n - 1).map(i => Bits.unproject(i.toInt, row)).map(sumValues(_)).min
      /*
      try {
        assert(num.gteq(sum, num.zero))
        assert(num.lteq(sum, sumValues(0)))
      } catch {
        case _ =>
          println(s"Row = $row Sum = ${num.toDouble(sum)}")
          sumValues.indices.foreach(i => println(s"$i -> ${sumValues(i)}"))
          throw new IllegalStateException
      }
      */

      sumValues(row) = num.min(sum, combMin)
    }
  }

  def fillMissing() = {
    val toSolve = Profiler("Solve Filter") {
      hamming_order.filter((!knownSums.contains(_)))
    }
    //println("Predicting values for " + toSolve.mkString(" "))

    Profiler("SetDefaultValueAll") {
      toSolve.foreach { r =>
        setDefaultValue(r)
      }
    }
  }

  def add(cols: Seq[Int], values: Array[T]) = {
    val eqnColSet = Bits.toInt(cols)
    fetchedCuboids = (eqnColSet -> values.map(num.toDouble(_))) :: fetchedCuboids
    val length = cols.length
    (0 until 1 << length).foreach { i0 =>
      val i = Bits.unproject(i0, eqnColSet)
      if (!knownSums.contains(i)) {
        val rowsToSum = (0 until 1 << length).filter(i2 => (i2 & i0) == i0)
        val rowsSum = rowsToSum.map(values(_)).sum
        knownSums += i
        sumValues(i) = rowsSum
      }
    }
  }

  def fastSolve() = {
    fillMissing()
    val result = sumValues.toArray
    var h = 1
    while (h < N) {
      (0 until N by h * 2).foreach { i =>
        (i until i + h).foreach { j =>
          result(j) = num.minus(result(j), result(j + h))
        }
      }
      h *= 2
    }
    solution = result.map(num.toDouble(_))
    result
  }

  def solve() = {
    fillMissing()
    val matA =
      Profiler("MatrixInit") {
        val matA = DenseMatrix.zeros[Double](1 << qsize, 1 << qsize)

        (0 until N).foreach { i =>
          (0 until N).foreach { j =>
            if ((i & j) == i)
              matA(i, j) = 1.0
          }
        }
        matA
      }
    val vals = DenseVector(sumValues.map(num.toDouble(_)): _*)
    val invA = Profiler("MatInv") {
      inv(matA)
    }
    //println(invA)
    val result = Profiler("MatMult") {
      invA * vals
    }

    def toT(d: Double) = {
      val i = d.toInt
      val f = (d-i)
      val prec = 1000000
      num.plus(num.fromInt(i), num.div(num.fromInt((f * prec).toInt), num.fromInt(prec)))
    }

    solution = result.toArray
    result
  }
}
