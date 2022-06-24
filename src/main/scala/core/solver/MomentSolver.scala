package core.solver

import util.{BigBinary, Bits, Profiler}

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

//primary moments are calculated assuming transformer is Moment1
abstract class MomentSolver[T: ClassTag](qsize: Int, batchMode: Boolean, transformer: MomentTransformer[T], primaryMoments: Seq[(Int, T)]) (implicit val num: Fractional[T]) {
  val N = 1 << qsize
  val solverName: String
  lazy val name = solverName + transformer.name

  var moments = Profiler.profile("Moments construct") {
    Array.fill(N)(num.zero)
  }
  Profiler.profile("Moments init") {
      primaryMoments.foreach { case (i, m) => moments(i) = m }
      moments = transformer.from1Moment(moments)
  }

  val knownSet = collection.mutable.BitSet()
  knownSet += 0
  (0 until qsize).foreach { b => knownSet += (1 << b) }

  val momentProducts = new Array[T](N)
  val knownMomentProducts = collection.mutable.BitSet()

  Profiler.profile("ExtraMoments init") {
    buildMomentProducts()
    moments.indices.filter(!knownSet(_)).foreach { i => moments(i) = num.times(momentProducts(i), moments(0)) }
  }
  val momentsToAdd = new ListBuffer[(Int, T)]()

  var solution = transformer.getValues(moments)

  def dof = N - knownSet.size

  def getStats = (dof, solution.clone())

  def buildMomentProducts() = {
    momentProducts(0) = num.one
    knownMomentProducts.clear()
    //Set products corresponding to singleton sets
    (0 until qsize).foreach { i =>
      val j = (1 << i)
      momentProducts(j) = if (knownSet(j)) num.div(moments(j) , moments(0)) else num.div(num.one, num.fromInt(2))
      knownMomentProducts += j
    }
    (0 until N).foreach { i =>
      if (!knownMomentProducts(i)) {
        var s = 1
        while (s < i) {
          //find any element s subset of i
          if ((i & s) == s) {
            //products(I) = products(I \ {s}) * products({s})
            momentProducts(i) = num.times(momentProducts(i - s), momentProducts(s))
            s = i + 1 //break
          } else {
            s = s << 1
          }
        }
        knownMomentProducts += i
      }
    }
  }

  def add(cols: Seq[Int], values: Array[T]) = {
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


