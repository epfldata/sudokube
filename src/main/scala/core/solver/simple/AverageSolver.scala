package core.solver.simple

import util.BitUtils

class AverageSolver(val qsize: Int, primaryMoments: Seq[(Int, Double)]) {
  val solverName = "AverageSolver"
  val N: Int = 1 << qsize
  var solution: Array[Double] = Array.fill(N)(0.0)
  val fetched = collection.mutable.ArrayBuffer[(Int, Array[Double])]()
  val total = primaryMoments.head._2
  assert(primaryMoments.head._1 == 0)
  val pmMap = primaryMoments.map { case (i, m) => i -> m / total }.toMap
  def weightFunction(q: Int) = (1 << q)
  def add(eqnColSet: Int, values: Array[Double]): Unit = {
    fetched += (eqnColSet, values)
  }
  def solve() =  {
    val array = Array.fill(N)(0.0)
    val totalWeights = fetched.map(x => weightFunction(x._1)).sum
    fetched.foreach { case (eqnColSet, values) =>
      val localWeight = weightFunction(eqnColSet)
      val colsLength = BitUtils.sizeOfSet(eqnColSet)
      val n0 = 1 << colsLength
      val localArray = Array.fill(N)(0.0)
      (0 until n0).foreach { i0 =>
        val i = BitUtils.unprojectIntWithInt(i0, eqnColSet)
        array(i) = values(i0)
      }
      var h = 1
      while(h < N) {
        if( (h & eqnColSet) == 0) { //Do only for missing dimensions
          val ph = pmMap(h)
          var i = 0
          while(i < N) {
            var j = 0
            while( j < h) {
              localArray(i + j + h) += ph * localArray(i + j)   // y = p*a + b
              localArray(i + j) -=  localArray(i + j + h)  //x = (1-p)*a + (-1)*b = a - (p*a + b)
              j += 1
            }
            i += (h << 1)
          }
        }
        h <<= 1
      }
      array.indices.foreach{ i =>
        array(i) += localArray(i) * localWeight / totalWeights
      }
    }
    solution = array
    solution
  }
}
