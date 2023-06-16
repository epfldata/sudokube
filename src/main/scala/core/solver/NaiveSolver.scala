package core.solver

//Just to store result for later use
class NaiveSolver {
  var solution: Array[Double] = null
  def add(result: Array[Double]): Unit = {
    solution = result
  }
  def solve() = solution
}
