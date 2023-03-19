package core.solver.wavelet

abstract class WaveletSolver(val solverName: String, val querySize: Int, val transformer: Transformer[Double]) {
  val N: Int = 1 << querySize
  var cuboids: Set[(Int, Array[Double])] = Set.empty
  var solution: Array[Double] = Array.empty

  def addCuboid(dimensions: Int, marginalValues: Array[Double]): Unit = {
    cuboids += ((dimensions, marginalValues))
  }

  def solve(): Array[Double]

}
