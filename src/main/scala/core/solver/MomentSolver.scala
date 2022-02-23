package core.solver

import util.Bits

import scala.collection.mutable.ListBuffer

abstract class MomentSolver(qsize: Int, batchMode: Boolean, transformer: MomentTransformer) {
  val N = 1 << qsize

  val moments = Array.fill(N)(0.0)
  val knownSet = collection.mutable.BitSet()

  val momentProducts = new Array[Double](N)

  val momentsToAdd = new ListBuffer[(Int, Double)]()
  var solution = Array.fill(N)(0.0)

  def dof = N - knownSet.size

  def getStats = (dof, solution.clone())

  def rowsToSum(n0: Int, i0: Int)

  def add(cols: Seq[Int], values: Array[Double]) = {
    val eqnColSet = Bits.toInt(cols)
    val n0 = 1 << cols.length

    val newMomentIndices = (0 until n0).map(i0 => i0 -> Bits.unproject(i0, eqnColSet)).
      filter({ case (i0, i) => !knownSet.contains(i) })

    if (newMomentIndices.size < cols.length) {
      // need less than log(n0) moments -- find individually
      newMomentIndices.foreach { case (i0, i) =>
        momentsToAdd += i -> transformer.getMoment(i0, values)
      }
    }
    else {
      //need more than log(n0) moments -- do moment transform and filter
      val cuboid_moments = transformer.getMoments(values)
      newMomentIndices.foreach { case (i0, i) =>
        momentsToAdd += i -> cuboid_moments(i0)
      }
    }
  }

  //only used in batchmode to denote end of fetch phase.
  def fillMissing()

  def solve(): Unit
}

