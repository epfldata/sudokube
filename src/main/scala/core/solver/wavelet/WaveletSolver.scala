package core.solver.wavelet

abstract class WaveletSolver(val solverName: String,
                             val querySize: Int,
                             val transformer: Transformer[Double],
                             val debug: Boolean = false) {
  /* N is the size of the dataset */
  val N: Int = 1 << querySize
  /**
   * Cuboid is stored a tuple of (variables, marginal value matrix)
   * Eg.
   * Dataset of size 3 is represented as a matrix ABC
   * (Seq(0, 1), Array(1.0, 2.0, 3.0, 4.0)) represents the cuboid  AB = [ [1.0, 2.0], [3.0, 4.0] ]
   * (Seq(1), Array(1.0, 2.0) represents the cuboid B = [ 1.0, 2.0 ]
   *
   * Seq(0, 1) represents the bit positions, from LSB to MSB, which index the marginal value matrix
   */
  var cuboids: Set[(Seq[Int], Array[Double])] = Set.empty

  var solution: Option[(Seq[Int], Array[Double])] = None

  def addCuboid(dimensions: Seq[Int], marginalValues: Array[Double]): Unit = {
    cuboids += ((dimensions, marginalValues))
  }

  def solve(): Array[Double]

}
