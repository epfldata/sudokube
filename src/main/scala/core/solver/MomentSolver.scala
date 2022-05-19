package core.solver

import util.{BigBinary, Bits, Profiler}

import scala.collection.mutable.ListBuffer

//primary moments are calculated assuming transformer is Moment1
abstract class MomentSolver(qsize: Int, batchMode: Boolean, transformer: MomentTransformer, primaryMoments: Seq[(Int, Double)]) {
  val N = 1 << qsize
  val solverName: String
  lazy val name = solverName + transformer.name

  val moments = Profiler.profile("Moments construct") {
    Array.fill(N)(0.0)
  }
  Profiler.profile("Moments init") {
    if (transformer == Moment1Transformer)
      primaryMoments.foreach { case (i, m) => moments(i) = m }
    else if (transformer == Moment0Transformer) {
      val pmMap = primaryMoments.toMap
      pmMap.foreach {
        case (0, m) => moments(0) = m
        case (i, m) => moments(i) = pmMap(0) - m
      }
    } else {
      ???
    }
  }
  val knownSet = collection.mutable.BitSet()
  knownSet += 0
  (0 until qsize).foreach { b => knownSet += (1 << b) }

  val momentProducts = new Array[Double](N)
  val knownMomentProducts = collection.mutable.BitSet()

  Profiler.profile("ExtraMoments init") {
    buildMomentProducts()
    moments.indices.filter(!knownSet(_)).foreach { i => moments(i) = momentProducts(i) * moments(0) }
  }
  val momentsToAdd = new ListBuffer[(Int, Double)]()

  var solution = Array.fill(N)(0.0)

  def dof = N - knownSet.size

  def getStats = (dof, solution.clone())

  def buildMomentProducts() = {
    momentProducts(0) = 1.0
    knownMomentProducts.clear()
    //Set products corresponding to singleton sets
    (0 until qsize).foreach { i =>
      val j = (1 << i)
      momentProducts(j) = if (knownSet(j)) moments(j) / moments(0) else 0.5
      knownMomentProducts += j
    }
    (0 until N).foreach { i =>
      if (!knownMomentProducts(i)) {
        var s = 1
        while (s < i) {
          //find any element s subset of i
          if ((i & s) == s) {
            //products(I) = products(I \ {s}) * products({s})
            momentProducts(i) = momentProducts(i - s) * momentProducts(s)
            s = i + 1 //break
          } else {
            s = s << 1
          }
        }
        knownMomentProducts += i
      }
    }
  }

  def add(cols: Seq[Int], values: Array[Double]) = {
    val eqnColSet = Bits.toInt(cols)
    val n0 = 1 << cols.length

    val newMomentIndices = (0 until n0).map(i0 => i0 -> Bits.unproject(i0, eqnColSet)).
      filter({ case (i0, i) => !knownSet.contains(i) })

    if (newMomentIndices.size < cols.length) {
      // need less than log(n0) moments -- find individually
      newMomentIndices.foreach { case (i0, i) =>
        momentsToAdd += i -> transformer.getMoment(i0, values)
        knownSet += i
      }
    }
    else {
      //need more than log(n0) moments -- do moment transform and filter
      val cuboid_moments = transformer.getMoments(values)
      newMomentIndices.foreach { case (i0, i) =>
        momentsToAdd += i -> cuboid_moments(i0)
        knownSet += i
      }
    }
  }

  //extrapolate missing moments
  def fillMissing()

  def solve() = {
    solution = transformer.getValues(moments)
    solution
  }
}


