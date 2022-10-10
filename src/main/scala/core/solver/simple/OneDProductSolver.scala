package core.solver.simple

class OneDProductSolver (val querySize: Int, primaryMoments: Seq[(Int, Double)]) {
  val solverName = "1D-ProductSolver"
  val N: Int = 1 << querySize
  var solution: Array[Double] = Array[Double]()
  val total =  primaryMoments.head._2
  assert(primaryMoments.head._1 == 0)
  val pmMap = primaryMoments.map { case (i, m) => i -> m / total }.toMap

  def solve() = {
    val array = Array.fill(N)(total)
    var h = 1
    while(h < N) {
      val ph = pmMap(h)
      var i = 0
      while(i < N) {
        var j = 0
        while(j < h) {
          array(i + j) *= (1 - ph)
          array(i + j + h) *= ph
          j += 1
        }
        i += (h << 1)
      }
      h <<= 1
    }
    solution = array
    solution
  }
}
