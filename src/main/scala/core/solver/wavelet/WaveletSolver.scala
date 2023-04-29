package core.solver.wavelet

import util.BitUtils

import scala.reflect.ClassTag

abstract class WaveletSolver[T]
(val solverName: String, val querySize: Int, val debug: Boolean = false)
(implicit fractional: Fractional[T], classTag: ClassTag[T]) {

  /* N is the size of the dataset */
  val N: Int = 1 << querySize
  val transformer: Transformer[T] = new HaarTransformer[T]()
  /**
   * Cuboid is stored a tuple of (variables, marginal value matrix)
   * Eg.
   * Dataset of size 3 is represented as a matrix ABC
   * (Seq(0, 1), Array(1.0, 2.0, 3.0, 4.0)) represents the cuboid  AB = [ [1.0, 2.0], [3.0, 4.0] ]
   * (Seq(1), Array(1.0, 2.0) represents the cuboid B = [ 1.0, 2.0 ]
   *
   * Seq(0, 1) represents the bit positions, from LSB to MSB, which index the marginal value matrix
   */
  var cuboids: Map[Seq[Int], Array[T]] = Map.empty

  var solution: Array[T] = _

  def addCuboid(dimensions: Int, marginalValues: Array[T]): Unit = {
    addCuboid(BitUtils.IntToSet(dimensions), marginalValues)
  }

  def addCuboid(dimensions: Seq[Int], marginalValues: Array[T]): Unit = {
    cuboids += ((dimensions, marginalValues))
  }

  def solve(): Array[T]

}
