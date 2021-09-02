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

  //trend1 : returns y value with slope ms for given x. m between -1 to 1. s is positive scale factor
  def t1(x: Int, m: Double, s: Double) = (x * m * s).toInt

  //trend2 : y follows random walk with slope ms. m between -1 to 1. s is step size. probability of success is given by m = (2p - 1)
  def t2(x: Int, m: Double, s: Double) = {
    val p = (m + 1)/2
    val mean = x * m * s
    val stddev = 2 * s * Math.sqrt(x* p * (1-p))
    (scala.util.Random.nextGaussian() * stddev + mean).toInt
  }

}


