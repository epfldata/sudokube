package frontend

object MomentComputer {

  /**
   * takes the count of the number of elements and their value aggregate, and compute an array containing the average of the array
   * @param count each element in the array contains the number of values aggregated to produce it
   * @param x1 simply the aggregation
   * @return an array containing the averages of each cell
   */
  def avg(count: Array[String], x1: Array[String] ): Array[String] = {
    if (count.length != x1.length) {
      Array.empty
    } else {
      val temp = new Array[String](count.length)
      count.indices.par.foreach(i => temp(i) = (x1(i).toLong/count(i).toLong).toString)
      temp
    }
  }

  /**
   * takes the count of the number of elements, their value aggregate and their square-value aggregate, and compute an array containing the variance of the array
   * @param count each element in the array contains the number of values aggregated to produce it
   * @param x1 simply the aggregation
   * @param x2 the aggregation with each cell individual value squared
   * @return an array containing the variances of each cell
   */
  def variance(count: Array[String], x1: Array[String], x2: Array[String] ): Array[String] = {
    if (count.length != x1.length || count.length != x2.length) {
      Array.empty
    } else {
      val temp = new Array[String](count.length)
      count.indices.par.foreach(i => temp(i) = (x2(i).toLong/count(i).toLong - math.pow(x1(i).toLong/count(i).toLong, 2)).toString)
      temp
    }
  }

  /**
   * takes the count of the number of elements, their value aggregate and their square-value aggregate, and compute an array containing the normals of the array
   * @param count each element in the array contains the number of values aggregated to produce it
   * @param x1 simply the aggregation
   * @param x2 the aggregation with each cell individual value squared
   * @return an array containing the value of each cell normalized
   */
  def normalize(count: Array[String], x1: Array[String], x2: Array[String] ): Array[String] = {
    if (count.length != x1.length || count.length != x2.length) {
      Array.empty
    } else {
      val mean = avg(count, x1)
      val varianceArray = variance(count, x1, x2)
      val temp = new Array[String](count.length)
      count.indices.par.foreach(i => temp(i) = ((x1(i).toDouble - mean(i).toDouble)/math.sqrt(varianceArray(i).toDouble)).toString)
      temp
    }
  }
}
