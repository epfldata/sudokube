package core.solver.moment

import util.{BitUtils, Profiler}

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

//primary moments are calculated assuming transformer is Moment1
abstract class MomentSolver[T: ClassTag](qsize: Int, batchMode: Boolean, transformer: MomentTransformer[T], primaryMoments: Seq[(Int, T)])(implicit val num: Fractional[T]) {
  val logN = qsize
  val N = 1 << logN
  val solverName: String
  lazy val name = solverName + transformer.name

  var moments: Array[T] = null
  val knownSet = collection.mutable.BitSet()
  var momentProducts: Array[T] = null
  val knownMomentProducts = collection.mutable.BitSet()

  val momentsToAdd = new ListBuffer[(Int, T)]()

  var solution: Array[T] = null

  init()

  def init(): Unit = {
    moments = Profiler.profile("Moments construct") {
      Array.fill(N)(num.zero)
    }
    Profiler.profile("Moments init") {
      primaryMoments.foreach { case (i, m) => moments(i) = m }
      moments = transformer.from1Moment(moments)
    }

    knownSet += 0
    (0 until qsize).foreach { b => knownSet += (1 << b) }

    Profiler.profile("ExtraMoments init") {
      buildMomentProducts()
      moments.indices.filter(!knownSet(_)).foreach { i => moments(i) = num.times(momentProducts(i), moments(0)) }
    }
    solution = transformer.getValues(moments)
  }


  def dof = N - knownSet.size

  def getStats = (dof, solution.clone())

  def buildMomentProducts() = {
    momentProducts = new Array[T](N)
    momentProducts(0) = num.one
    knownMomentProducts.clear()
    //Set products corresponding to singleton sets
    (0 until qsize).foreach { i =>
      val j = (1 << i)
      momentProducts(j) = if (knownSet(j)) num.div(moments(j), moments(0)) else num.div(num.one, num.fromInt(2))
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

  def add(eqnColSet: Int, values: Array[T]): Unit = {
    val colsLength = BitUtils.sizeOfSet(eqnColSet)
    val n0 =  1 << colsLength

    val newMomentIndices = (0 until n0).map(i0 => i0 -> BitUtils.unprojectIntWithInt(i0, eqnColSet)).
      filter({ case (i0, i) => !knownSet.contains(i) })

    if (newMomentIndices.size < colsLength) {
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

  def solve(hn: Boolean = true) = {
    solution = transformer.getValues(moments, hn)
    solution
  }
}


