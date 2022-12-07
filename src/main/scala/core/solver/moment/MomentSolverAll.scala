package core.solver.moment

import combinatorics.Combinatorics
import core.solver.moment.Strategy.{CoMoment, Strategy}
import util.{BigBinary, BitUtils, Profiler}

import scala.reflect.ClassTag

object Strategy extends Enumeration {
  type Strategy = Value
  val Avg, Avg2, Cumulant, Cumulant2, CoMoment, CoMomentFrechet, CoMoment3, MeanProduct, CoMoment4, CoMoment5, CoMoment5Slice, CoMoment5SliceTrieStore,  Zero, HalfPowerD, FrechetUpper, FrechetMid, LowVariance = Value
}

/**
 * Finds approximate values of cuboids using moments under some uniformity assumptiom.
 * Different strategies model the uniformity assumption differently
 *
 * @param qsize    Number of bits in query
 * @param strategy Strategy to be used for solving
 * @tparam T Type of the value (Double / Rational)
 */
class MomentSolverAll[T: ClassTag](val qsize: Int, val strategy: Strategy = CoMoment)(implicit num: Fractional[T]) {
  import Strategy._

  val N = 1 << qsize
  assert(qsize < 31)
  //TODO: Check if we really need hamming order for any algorithm?
  val hamming_order = (0 until N) //Profiler(s"USolve hamming sort $strategy"){(0 until N).sortBy(i => BigBinary(i).hamming_weight)}

  //Both sumValues and moments store the moments. I needed a separate copy to compare two strategies so we have two arrays storing the same thing
  //TODO: Merge sumValues and moments
  val sumValues = Array.fill(N)(num.zero)
  val moments = Array.fill(N)(num.zero)

  //Which moments do we know ?
  val knownSums = collection.mutable.BitSet()

  val qArray = Array.fill(N)(Array.fill(qsize)(num.zero))

  //Stores products of 1D moments. p1 = m1/m0  p2 = m2/m0, p3 = p1 * p2
  val meanProducts = new Array[T](N)
  val knownMeanProducts = collection.mutable.BitSet()

  //Store cumulants. Only used by Cumulant strategy
  val cumulantMap = Array.fill(N)(num.zero)
  val knownCumulants = collection.mutable.BitSet()

  //Stores which cuboids have been fetched and what data they contain
  var fetchedCuboids = List[(Int, Array[Double])]()

  //Final result of the query will be updated here. Initially all values are 0, making the error 1
  var solution = Array.fill(N)(0.0)
  val lattice = new Lattice[T](qsize)

  //Used in online-algorithm to get intermediate results. Returns current degrees of freedom and current solution
  def getStats = (dof, solution.clone())

  def verifySolution() = {
    //solution.indices.foreach(i => if (solution(i) < 0) println(s"solution[$i] = ${solution(i)}"))
    //assert(solution.map(_ >= 0).reduce(_ && _))

    fetchedCuboids.foreach {
      case (cols, values) =>
        //println("Checking cuboid " + cols)
        val projection = (0 until solution.length).groupBy(i => BitUtils.projectIntWithInt(i, cols)).mapValues {
          idxes => idxes.map(solution(_)).sum
        }.toSeq.sortBy(_._1).map(_._2)
        values.zip(projection).zipWithIndex.foreach { case ((v, p), i) => if (Math.abs(v - p) > 0.0001) println(s"$i :: $v != $p") }
        assert(values.zip(projection).map { case (v, p) => Math.abs(v - p) <= 0.0001 }.reduce(_ && _))
    }
  }

  //Degrees of freedom
  def dof = N - knownSums.size

  //Upper bound on error for different strategy. Too high to be useful.
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

  //Lower Frechet bound on the value of some moment. Not tight
  def lowerBound(row: Int) = {
    val n = BigBinary(row).hamming_weight
    val combSum = Combinatorics.mk_comb_bi(n, n - 1).map(i => BitUtils.unprojectIntWithInt(i.toInt, row)).map(sumValues(_)).sum
    num.max(num.zero, num.plus(num.times(num.fromInt(1 - n), sumValues(0)), combSum))
  }

  //Upper Frechet  bound on the value of some moment. No moment should be higher than moments of its subsets
  def upperBound(row: Int) = {
    val n = BigBinary(row).hamming_weight
    val combMin = Combinatorics.mk_comb_bi(n, n - 1).map(i => BitUtils.unprojectIntWithInt(i.toInt, row)).map(sumValues(_)).min
    combMin
  }

  //Sets the default value of a moment to be equal to average of its immediate predecessors.
  def getDefaultValueAvg(row: Int) = {
    val n = BigBinary(row).hamming_weight
    val combSum = Combinatorics.mk_comb_bi(n, n - 1).map(i => BitUtils.unprojectIntWithInt(i.toInt, row)).map(sumValues(_)).sum
    num.div(combSum, num.fromInt(2 * n))
  }

  //Sets default value of a moment to weighted average of all its predecessors
  def getDefaultValueAvg2(row: Int) = {
    val n = BigBinary(row).hamming_weight
    val dimSums = (0 until n).map{ k =>
      val combSum = Combinatorics.mk_comb_bi(n, k).map(i => BitUtils.unprojectIntWithInt(i, row)).map(sumValues(_)).sum
      val div =  (1 << (n-k))
      num.div(combSum, num.fromInt(div))
    }.sum
    num.div(dimSums, num.fromInt((1<<n)-1))
  }
  def getDefaultValueCumulant(row: Int) = {
    //Add element a to all partitions in parts
    def addElem(a: Int, parts: List[List[Int]]) = {
      def rec(before: List[List[Int]], cur: List[List[Int]], acc: List[List[List[Int]]]): List[List[List[Int]]] = cur match {
        case Nil => (List(a) :: before) :: acc
        case h :: t =>
          val acc2 = ((a :: h) :: (before ++ t)) :: acc
          rec(before :+ h, t, acc2)
      }

      rec(Nil, parts, Nil)
    }

    //Find all partitions of list l = find all partitions of tail and then add head to every partition.
    def allParts(l: List[Int]): List[List[List[Int]]] = l match {
      case h :: Nil => List(List(List(h)))
      case h :: t => {
        val pt = allParts(t)
        pt.flatMap(p => addElem(h, p))
      }
    }

    val colSet = BitUtils.IntToSet(row)
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
      case h :: t =>
        val acc2 = if (knownSums(a + h)) ((a + h) :: (before ++ t)) :: acc else acc
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
    if (parts.size > 1000) {
      //println(parts.size)
      val parts2 = parts.map(p => p.toSet).toSet
      //println(parts2.size)
      assert(parts.map(part => part.map(x => knownSums(x)).reduce(_ && _)).reduce(_ && _))
      //parts.foreach( x => println(x.size))
      ()
    }
    val sum = parts.map { partition =>
      partition.map { part => cumulantMap(part) }.product
    }.sum
    num.times(sum, sumValues(0))
  }

  /**
   * Incremental version of Comoment strategy.
   * mu(row) = v - moments(row)
   * for each J superset of row :
   * moments(J) += mu(row) * product(J \ row)
   */
  def changeMoment(row: Int, v: T): Unit = {
    val delta = num.minus(v, moments(row))
    if (!num.equiv(delta, num.zero)) {
      if (strategy != MeanProduct) {
        moments(row) = v
        //if(true) {
        val supersets = Profiler(s"FindSuperSet $strategy") {
          (row + 1 until N).filter(i => (i & row) == row)
        }
        //if(N-row < 20)
        //println(s"row $row Superset count = "+supersets.size + " == " + supersets.mkString(" "))
        Profiler(s"IncrementSuperSet $strategy") {
          supersets.foreach { i =>
            moments(i) = num.plus(moments(i), num.times(delta, meanProducts(i - row)))
            //println(s"m[$i] += (sv[$row] - m[$row]) * mp[${i-row}]")
          }
        }
      } else {
        lattice.nodes(row).value = delta
      }
    }
  }

  def propagateDelta(start: Int) = {
    import lattice._
    val queue = collection.mutable.Queue[Node]()
    nodes(start).visited = true
    queue += nodes(start)
    var count = 0
    var str = ""
    while (!queue.isEmpty) {
      val n = queue.dequeue()
        val i = n.key
        count += 1
        str += (" " + i.toString)
        n.children.foreach { c =>
          val j = c.key
          val b = j - i
          val m = meanProducts(b)
          val v = num.times(n.value, m)
          c.value = num.plus(c.value, v)
        }
        moments(i) = num.plus(moments(i), n.value)
        n.value = num.zero
        n.visited = false
        val newc = n.children.filter(!_.visited)
        newc.foreach(_.visited = true)
        queue ++= newc
      }
    //if(lattice.N-start < 20)
    //println(s"row $start Child count = "+ count + "children = " + str)
  }

  /**
   * m(J) =  sum_{emptyset \neq K subsetof J} (-1)^{ |K| + 1 } * m(J\K) * products(K)
   */
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
          val combs = Profiler(s"unproject Comb $strategy") {
            projcombs.map(i => BitUtils.unprojectIntWithInt(i, row))
          }
          val sign = if ((k % 2) == 1) num.one else num.negate(num.one)
          //println(s"$sign * ${combs.map{ i => s"sv[${row-i}] * mp[$i]"}.mkString("[", " + ", "]")}")
          //TODO: Can simplify further if parents were unknown and default values were used for them
          Profiler(s"Term Mult $strategy") {
            num.times(sign, combs.map { i =>
              num.times(sumValues(row - i), meanProducts(i))
            }.sum)
          }


        }.sum
    }
    if (withUpperBound) num.max(num.zero, num.min(sum, upperBound(row))) else sum
  }

  def getDefaultValueLowVariance(row: Int) = {
    val n = BigBinary(row).hamming_weight
    val N1 = 1 << n

    val array = new Array[T](N1)
    array.indices.foreach { i =>
      array(i) = if (i == array.indices.last) num.zero else {
        val j = BitUtils.unprojectIntWithInt(i, row)
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
      case Avg2 => getDefaultValueAvg2(row)
      case Cumulant => getDefaultValueCumulant(row)
      case Cumulant2 => ??? //getDefaultValueCumulant2(row)
      case CoMoment => getDefaultValueCoMoment(row, false)
      case CoMomentFrechet => getDefaultValueCoMoment(row, true)
      case CoMoment3 | MeanProduct => getDefaultValueCoMoment(row, true)
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
    meanProducts(0) = num.one
    //Set products corresponding to singleton sets
    (0 until qsize).foreach { i =>
      val j = (1 << i)
      meanProducts(j) = if (knownSums(j)) num.div(sumValues(j), sumValues(0)) else num.div(num.fromInt(1), num.fromInt(2))
      knownMeanProducts += j
    }
    (0 until N).foreach { i =>
      if (!knownMeanProducts(i)) {
        var s = 1
        while (s < i) {
          //find any element s subset of i
          if ((i & s) == s) {
            //products(I) = products(I \ {s}) * products({s})
            meanProducts(i) = num.times(meanProducts(i - s), meanProducts(s))
            s = i + 1 //break
          } else {
            s = s << 1
          }
        }
        knownMeanProducts += i
      }
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
    hamming_order.filter(x => knownSums.contains(x) && !knownCumulants.contains(x)).foreach { row =>
      val partitions = getPart(row).filter(_.size > 1)
      val sum = partitions.map { partition => partition.map(cumulantMap).product }.sum
      val sumStr = partitions.map { partition => partition.mkString("*") }.mkString(" + ")
      val res = num.minus(num.div(sumValues(row), sumValues(0)), sum)
      //println(s" $row = $sumStr  $sum $res")

      cumulantMap(row) = res
      knownCumulants += row
    }
  }

  /**
   * Interpolates unknown moments using known moments depending on strategy
   */
  def fillMissing() = {

    //unknown moments
    val toSolve = Profiler(s"Solve Filter $strategy") {
      hamming_order.filter((!knownSums.contains(_)))
    }
    //println("Known = " + knownSums.size + " Unknown = " + toSolve.size)
    //println("Predicting values for " + toSolve.mkString(" "))

    //build products of moments. Need to be cleared and rebuilt for online algorithms as initial condition of knowning all singleton moments may not be satisfied
    Profiler(s"MeanProducts $strategy") {
      knownMeanProducts.clear()
      buildMeanProduct()
    }

    //Profiler("Build Cumulant") {
    //  buildCumulant()
    //}

    //For strategies other than CoMoment3, we iterate over all missing moments and set values
    if (strategy != CoMoment3 && strategy != MeanProduct && strategy != CoMoment4) {
      Profiler(s"SetDefaultValueAll $strategy") {
        toSolve.foreach { r =>
          setDefaultValue(r)
        }
      }
    }
    if (strategy == CoMoment3 || strategy == MeanProduct) {
      //for the comoment3 strategy, we don't calcule the value of missing momnets, instead known moments trigger updates to superset moments.
      Profiler(s"IncrementMomentAll $strategy") {
        val known = hamming_order.filter(knownSums)
        known.foreach { r =>
          changeMoment(r, sumValues(r))
        }
        if(strategy == MeanProduct)
          Profiler(s"Propagate Delta $strategy") {
            propagateDelta(0)
          }
        /*
        Change moment calculates the moment and stores in the array "moment".
         Move it back to the array "sumValues"
         */
        toSolve.foreach { r =>
          val v1 = moments(r)

          sumValues(r) = v1

        }
      }

    }
    if(strategy == CoMoment4){
      fillMissingComoment4()
    }

    //println(sumValues.map(_.asInstanceOf[Double].toLong).mkString("", " ", "\n"))
  }
  def fillMissingComoment4() = {
    (0 until N).foreach{i =>
      val bits = BitUtils.IntToSet(i)
      val w = bits.length
      val parents = bits.map(b => i - (1<<b))
      if(!knownSums(i)) {
          val qsum = parents.map{k => num.times(qArray(k)(w-1), meanProducts(i-k))}.sum
          //println(s"m[$i] = $qsum")
          sumValues(i) = qsum
      }
      (w + 1 to qsize).map { j =>
        val qsum = parents.map{k =>num.times(qArray(k)(j-1), meanProducts(i-k))}.sum
        val q = num.minus(sumValues(i), num.div(qsum, num.fromInt(j-w+1)))
        //println(s"q[$i][$j] = $q")
        qArray(i)(j-1) = q
      }
    }
  }

  /** Adds a cuboid data to solver */
  def add(eqnColSet: Int, values: Array[T]): Unit = {

    fetchedCuboids = (eqnColSet -> values.map(num.toDouble(_))) :: fetchedCuboids
    //println(s"Fetch $eqnColSet")
    //println(values.map(_.asInstanceOf[Double].toLong).mkString("", " ", "\n"))
    val length = BitUtils.sizeOfSet(eqnColSet)

    //calculate any previously unknown moments from the cuboid
    (0 until 1 << length).foreach { i0 =>
      val i = BitUtils.unprojectIntWithInt(i0, eqnColSet)
      if (!knownSums.contains(i)) {
        val rowsToSum = (0 until 1 << length).filter(i2 => (i2 & i0) == i0)
        val rowsSum = rowsToSum.map(values(_)).sum
        knownSums += i
        sumValues(i) = rowsSum
        //println(s"Add $i = ${rowsSum.asInstanceOf[Double].toLong}")
      }
    }
  }

  /**
   * Converts moments to values using algorithm similar to FFT
   */
  def fastSolve() = {
    val result = sumValues.clone()
    var h = 1
    while (h < N) {
      (0 until N by h * 2).foreach { i =>
        (i until i + h).foreach { j =>
          val diff = num.minus(result(j), result(j + h))
          strategy match {
            //special handling of negative values for these algorithms
            //this can only result in lower error (at least, I think so)
            case  _ =>
              if (num.lt(diff, num.zero)) {
                result(j + h) = result(j)
                result(j) = num.zero
              } else if (num.lt(result(j + h), num.zero)) {
                result(j + h) = num.zero
              } else
                result(j) = diff
          }
        }
      }
      h *= 2
    }
    solution = result.map(num.toDouble(_))
    result
  }

}
