package core.solver

import util.{Bits, Profiler}

import scala.collection.mutable.ListBuffer

//primary moments are calculated assuming transformer is Moment1
abstract class MomentSolver(qsize: Int, batchMode: Boolean, transformer: MomentTransformer, primaryMoments: Seq[(Int, Double)]) {
  val N = 1 << qsize
  val solverName: String
  lazy val name = solverName + transformer.name

  val moments = Array.fill(N)(0.0)
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

  val knownSet = collection.mutable.BitSet()

  val momentProducts = new Array[Double](N)
  val knownMomentProducts = collection.mutable.BitSet()

  var shouldBuildMomentProducts = true
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

  //only used in batchmode to denote end of fetch phase.
  def fillMissing()

  def solve() = {
    solution = transformer.getValues(moments)
    solution
  }
}

class CoMoment4Solver(qsize: Int, batchmode: Boolean, transformer: MomentTransformer, primaryMoments: Seq[(Int, Double)]) extends MomentSolver(qsize, batchmode, transformer, primaryMoments) {
  val qArray = Array.fill(N)(Array.fill(qsize)(0.0))

  override val solverName: String = "Comoment4"

  override def fillMissing(): Unit = {
    Profiler.profile("AddMoments") {
      momentsToAdd.foreach { case (i, m) =>
        moments(i) = m
      }
    }
    Profiler.profile("MomentProduct") {
      val allsingleton = (0 until qsize).map { b => knownSet(1 << b) }.reduce(_ && _)
      if (shouldBuildMomentProducts)
        buildMomentProducts()
      shouldBuildMomentProducts = !allsingleton
    }
    (0 until N).foreach { i =>

      val bits = Bits.fromInt(i)
      val w = bits.length
      val parents = bits.map(b => i - (1 << b))
      Profiler("part1") {
        if (!knownSet(i)) {
          val qsum = Profiler.profile("qsum1") {
            parents.map { k => qArray(k)(w - 1) * momentProducts(i - k) }.sum
          }
          //println(s"m[$i] = $qsum")
          moments(i) = qsum
        }
      }
      Profiler("part2") {
        (w + 1 to qsize).map { j =>
          val qsum = Profiler.profile("qsum2") {
            parents.map { k => qArray(k)(j - 1) * momentProducts(i - k) }.sum
          }
          //q[S][j-1] = m[S] - 1/(j+1-|S|) sum_s [ q[S-s][j-1] ]
          val q = moments(i) - (qsum / (j - w + 1))
          //println(s"q[$i][$j] = $q")
          qArray(i)(j - 1) = q
        }
      }
    }
  }
}

