//package ch.epfl.data.sudokube
package frontend


/** sampling functions that return a number in 0 .. (range - 1) */
object Sampling {
  // uniform
  def f1(range: Int) = scala.util.Random.nextInt(range)

  // skewed towards 0
  def f2(range: Int) = {
    val rh = range.toDouble / 5
    val sample0 = (scala.util.Random.nextGaussian) * rh
    math.min(math.max(0, sample0), range - 1).toInt
  }
}


