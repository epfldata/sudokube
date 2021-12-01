package core.solver

import breeze.linalg.{DenseMatrix, DenseVector, inv}
import combinatorics.Combinatorics
import core.solver.Strategy.{Strategy, CoMoment}
import util.{BigBinary, Bits, Profiler, Util}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object Strategy extends Enumeration {
  type Strategy = Value
  val Avg, Cumulant,Cumulant2, CoMoment, CoMomentFrechet, CoMoment3, Zero, HalfPowerD, FrechetUpper, FrechetMid, LowVariance = Value
}

class UniformSolver[T: ClassTag](val qsize: Int, val strategy: Strategy = CoMoment)(implicit num: Fractional[T]) {
  var allowNegative = false

  import Strategy._

  val N = 1 << qsize
  assert(qsize < 31)
  val hamming_order = (0 until N).sortBy(i => BigBinary(i).hamming_weight)

  val sumValues = Array.fill(N)(num.zero)
  val moments = Array.fill(N)(num.zero)
  val knownSums = collection.mutable.BitSet()

  val meanProducts = new Array[T](N)
  val knownMeanProducts = collection.mutable.BitSet()

  val cumulantMap = Array.fill(N)( num.zero)
  val knownCumulants = collection.mutable.BitSet()

  var fetchedCuboids = List[(Int, Array[Double])]()
  var solution = Array.fill(N)(0.0)

  def getStats = (dof, solution.clone())

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
    hamming_order.foreach { row =>
      if (knownSums(row))
        delta(row) = num.zero
      else {
        val n = BigBinary(row).hamming_weight
        val lb = num.abs(num.minus(lowerBound(row), sumValues(row)))
        val ub = num.abs(num.minus(upperBound(row), sumValues(row)))
        delta(row) = num.times(num.max(lb, ub), num.fromInt(1 << n))
      }
    }
    //var h = 1
    //while (h < N) {
    //  (0 until N by h * 2).foreach { i =>
    //    (i until i + h).foreach { j =>
    //      delta(j) = num.minus(delta(j), delta(j + h))
    //    }
    //  }
    //  h *= 2
    //}
    //num.toDouble(num.div(delta.map(num.abs).sum,sumValues(0)))
    num.toDouble(num.div(delta.sum, sumValues(0)))
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

  def getDefaultValueAvg(row: Int) = {
    val n = BigBinary(row).hamming_weight
    val combSum = Combinatorics.mk_comb_bi(n, n - 1).map(i => Bits.unproject(i.toInt, row)).map(sumValues(_)).sum
    num.div(combSum, num.fromInt(2 * n))
  }

  def getDefaultValueCumulant(row: Int) = {
    def addElem(a: Int, parts: List[List[Int]]) = {
      def rec(before: List[List[Int]], cur: List[List[Int]], acc: List[List[List[Int]]]): List[List[List[Int]]] = cur match {
        case Nil => (List(a) :: before) :: acc
        case h :: t =>
          val acc2 = ((a :: h) :: (before ++ t)) :: acc
          rec(before :+ h, t, acc2)
      }

      rec(Nil, parts, Nil)
    }

    def allParts(l: List[Int]): List[List[List[Int]]] = l match {
      case h :: Nil => List(List(List(h)))
      case h :: t => {
        val pt = allParts(t)
        pt.flatMap(p => addElem(h, p))
      }
    }

    val colSet = Bits.fromInt(row)
    val partitions = allParts(colSet)
    val sum = partitions.map {
      case parts if parts.length > 1 =>
        val n = parts.length
        val sign = if (n % 2 == 0) 1 else -1
        assert(n <= 13) //TODO: Expand to beyond Int limits
        val fact = Combinatorics.factorial(n - 1).toInt

        val prod = parts.map { case p =>
          val r2 = p.map(1 << _).sum
          num.div(sumValues(r2), sumValues(0))
        }.product
        num.times(num.fromInt(fact * sign), prod)
      case _ => num.zero
    }.sum
    num.times(sum, sumValues(0))
  }

  val partitionMap = collection.mutable.Map[Int, List[List[Int]]]()

  def addElem(a: Int, partition: List[Int]) = {
    def rec(before: List[Int], after: List[Int], acc: List[List[Int]]): List[List[Int]] = after match {
      case Nil => (a :: before) :: acc
      case h :: t  =>
        val acc2 = if(knownSums(a + h)) ((a + h) :: (before ++ t)) :: acc else acc
        rec(before :+ h, t, acc2)

    }
    rec(Nil, partition, Nil)
  }

  def getPart(s: Int): List[List[Int]] = ???
  /*
  def getPart(s: Int): List[List[Int]] =  if(partitionMap.isDefinedAt(s)) partitionMap(s) else {

    var a = 1
    while((s & a) == 0 || !knownSums(a))
      a = a << 1

    val partitions = if(s==a)
      List(List(a))
    else {
      getPart(s-a).flatMap{p => addElem(a, p)}
    }
    //println(s, partitions)
    partitionMap += s -> partitions
    partitions
  }
*/
  def getDefaultValueCumulant2(row: Int) = {
    val parts = getPart(row)
    if(parts.size > 1000) {
      println(parts.size)
      val parts2 = parts.map(p => p.toSet).toSet
      println(parts2.size)
      assert(parts.map(part => part.map(x => knownSums(x)).reduce(_ && _)).reduce(_ &&_))
      //parts.foreach( x => println(x.size))
      ()
    }
   val sum = parts.map{ partition =>
      partition.map{part => cumulantMap(part)}.product
    }.sum
    num.times(sum, sumValues(0))
  }

 def changeMoment(row: Int, v: T): Unit = {
   val delta = num.minus(v, moments(row))
   if(!num.equiv(delta, num.zero)) {
     moments(row) = v
     val supersets = Profiler("FindSuperSet"){(row + 1 until N).filter(i => (i & row) == row)}
     Profiler("IncrementSuperSet") {
       supersets.foreach { i =>
         moments(i) = num.plus(moments(i), num.times(delta, meanProducts(i - row)))
         //println(s"m[$i] += (sv[$row] - m[$row]) * mp[${i-row}]")
       }
     }
   }
 }

  def getDefaultValueCoMoment(row: Int, withUpperBound: Boolean) = {
    val n = BigBinary(row).hamming_weight
    val N1 = 1 << n


    //Special case for means
    val sum = Profiler.noprofile(s"SDV n$n") {
      if (n == 1)
        num.div(sumValues(0), num.fromInt(2))
      else
        (1 to n).map { k =>

            //WARNING: Ensure that query does not involve more than 30 bits
            val projcombs = Profiler.noprofile("mkComb") {
              Combinatorics.comb2(n, k)
            }
            val combs = Profiler("unproject Comb"){projcombs.map(i => Bits.unproject(i, row))}
            val sign = if ((k % 2) == 1) num.one else num.negate(num.one)
            //println(s"$sign * ${combs.map{ i => s"sv[${row-i}] * mp[$i]"}.mkString("[", " + ", "]")}")
            //TODO: Can simplify further if parents were unknown and default values were used for them
            Profiler("Term Mult"){num.times(sign, combs.map { i =>
              num.times(sumValues(row - i), meanProducts(i))
            }.sum)}


        }.sum
    }
    if (withUpperBound) num.max(num.zero, num.min(sum,  upperBound(row))) else sum
  }

  def getDefaultValueLowVariance(row: Int) = {
    val n = BigBinary(row).hamming_weight
    val N1 = 1 << n

    val array = new Array[T](N1)
    array.indices.foreach { i =>
      array(i) = if (i == array.indices.last) num.zero else {
        val j = Bits.unproject(i, row)
        sumValues(j)
      }
    }
    var h = 1
    while (h < N1) {
      (0 until N1 by h * 2).foreach { i =>
        (i until i + h).foreach { j =>
          if (h == 1) {
            array(j) = num.minus(array(j), array(j + h))
            array(j + h) = num.negate(array(j + h))
          } else {
            array(j) = num.minus(array(j + h), array(j))
          }
        }
      }
      h *= 2
    }
    num.div(array.sum, num.fromInt(N1))
  }

  def setDefaultValue(row: Int) = {
    val n = BigBinary(row).hamming_weight
    val N1 = 1 << n

    val value = strategy match {
      case Avg => getDefaultValueAvg(row)
      case Cumulant => getDefaultValueCumulant(row)
      case Cumulant2 => ??? //getDefaultValueCumulant2(row)
      case CoMoment => getDefaultValueCoMoment(row, false)
      case CoMomentFrechet => getDefaultValueCoMoment(row, true)
      case CoMoment3 => getDefaultValueCoMoment(row, true)
      case Zero => num.zero
      case HalfPowerD => num.div(sumValues(0), num.fromInt(1 << n))
      case FrechetUpper => upperBound(row)
      case FrechetMid => num.div(num.plus(upperBound(row), lowerBound(row)), num.fromInt(2))
      case LowVariance => getDefaultValueLowVariance(row)
    }
    sumValues(row) = value
    //println(s"Filling $row = ${num.toDouble(value).toLong}")
  }

  def buildMeanProduct() = {
    val sizeOne = (0 until qsize).map { i =>
      val j = (1 << i)
      meanProducts(j) = num.div(sumValues(j), sumValues(0))
      j
    }
    var curSize = 1
    var curIdx = sizeOne
    while (curSize < qsize) {
      val newIdx = curIdx.flatMap { a =>
        sizeOne.flatMap { b =>
          val c = a + b
          if (((a & b) == 0) && !knownMeanProducts.contains(c)) {
            knownMeanProducts += c
            meanProducts(c) = num.times(meanProducts(a), meanProducts(b))
            Some(c)
          } else None
        }
      }
      curIdx = newIdx
      curSize += 1
    }
  }

  def buildCumulant(): Unit = {
    knownCumulants += 0
    cumulantMap(0) = num.one
    val sizeOne = (0 until qsize).map { i =>
      val j = (1 << i)
      cumulantMap(j) = num.div(sumValues(j), sumValues(0))
      knownCumulants += j
      j
    }
    hamming_order.filter(x => knownSums.contains(x) && !knownCumulants.contains(x)).foreach { row  =>
      val partitions = getPart(row).filter(_.size > 1)
      val sum = partitions.map { partition => partition.map(cumulantMap).product}.sum
      val sumStr = partitions.map{ partition => partition.mkString("*")}.mkString(" + ")
      val res = num.minus(num.div(sumValues(row), sumValues(0)), sum)
      //println(s" $row = $sumStr  $sum $res")

      cumulantMap(row) = res
      knownCumulants += row
    }
  }

  def fillMissing() = {

    import core.solver.Strategy.Strategy

    val toSolve = Profiler("Solve Filter") {
      hamming_order.filter((!knownSums.contains(_)))
    }
    //println("Predicting values for " + toSolve.mkString(" "))

    Profiler("MeanProducts") {
      knownMeanProducts.clear()
      buildMeanProduct()
    }

    //Profiler("Build Cumulant") {
    //  buildCumulant()
    //}

    if(strategy != CoMoment3)
    {
      Profiler("SetDefaultValueAll") {
        toSolve.foreach { r =>
          setDefaultValue(r)
        }
      }
    }
    if(strategy == CoMoment3)  {
      Profiler("IncrementMomentAll") {
        val known = hamming_order.filter(knownSums)
        known.foreach { r =>
          changeMoment(r, sumValues(r))
        }
        toSolve.foreach{r =>
          val v1 = moments(r)

          var v2 = v1
          var s = 1
          while(s < r && num.gt(v2,  num.zero)) {
            if((s & r) == s)
            v2 = num.min(v2, sumValues(r-s))
            s = s << 1
          }
          if(num.lt(v2, num.zero))
            v2 = num.zero

          //val v3 = getDefaultValueCoMoment(r, true)
          //assert(num.equiv(v3, v2))

          //val prec = 1000000
         //val diffp = num.div(num.fromInt(prec+1), num.fromInt(prec))
         //val diffm = num.div(num.fromInt(prec-1), num.fromInt(prec))

          //assert(num.lteq(sumValues(r), num.times(v2, diffp)))
          //assert(num.gteq(sumValues(r), num.times(v2, diffm)))

          sumValues(r) = v2
          //println(s"Filling2 $r = ${num.toDouble(v1).toLong}")
        }
      }

    }

    //println(sumValues.map(_.asInstanceOf[Double].toLong).mkString("", " ", "\n"))
  }

  def add(cols: Seq[Int], values: Array[T]) = {
    val eqnColSet = Bits.toInt(cols)
    fetchedCuboids = (eqnColSet -> values.map(num.toDouble(_))) :: fetchedCuboids
    //println(s"Fetch $eqnColSet")
    //println(values.map(_.asInstanceOf[Double].toLong).mkString("", " ", "\n"))
    val length = cols.length
    (0 until 1 << length).foreach { i0 =>
      val i = Bits.unproject(i0, eqnColSet)
      if (!knownSums.contains(i)) {
        val rowsToSum = (0 until 1 << length).filter(i2 => (i2 & i0) == i0)
        val rowsSum = rowsToSum.map(values(_)).sum
        knownSums += i
        sumValues(i) = rowsSum
        //println(s"Add $i = ${rowsSum.asInstanceOf[Double].toLong}")
      }
    }
  }

  def fastSolve() = {
    val result = sumValues.clone()
    var h = 1
    while (h < N) {
      (0 until N by h * 2).foreach { i =>
        (i until i + h).foreach { j =>
          val diff = num.minus(result(j), result(j + h))
          strategy match {
            case CoMomentFrechet | Cumulant2 | CoMoment3 =>
              if (num.lt(diff, num.zero)) {
                result(j + h) = result(j)
                result(j) = num.zero
              } else if (num.lt(result(j + h), num.zero)) {
                result(j + h) = num.zero
              } else
                result(j) = diff
            case _ => result(j) = diff
          }
        }
      }
      h *= 2
    }
    solution = result.map(num.toDouble(_))
    result
  }

  def solve() = {
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
      val f = (d - i)
      val prec = 1000000
      num.plus(num.fromInt(i), num.div(num.fromInt((f * prec).toInt), num.fromInt(prec)))
    }

    solution = result.toArray
    result
  }
}
