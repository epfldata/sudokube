package core.materialization
import combinatorics.Combinatorics
import util.Util

/**
 * @param n      data dimensions, top cube: (0, ..., n - 1)
 * @param rf     redundancy factor, multiplier for the number
 *               of cubes for each size < n
 * @param base   basis of power with which cuboid counts are computed.
 *               a large base shifts the bulk of the storage costs down to
 *               small-dimension cuboids. A large rf shifts it to
 *               high-dimension cuboids.
 * @param mindim do not materialize cuboids of dimensionality less than
 *               mindim (default 0)
 */

@SerialVersionUID(1L)
case class OldRandomizedMaterializationStrategy(
                                            _n_bits: Int,
                                            rf: Double,
                                            base: Double,
                                            mindim: Int = 0
                                          ) extends MaterializationStrategy(_n_bits) {

  /** build that many cuboids with d dimensions.
   * @example {{{
   *  def plot(n: Int, rf: Double, base: Double, mindim: Int = 0) = {
   *      (0 to n).map(d => { print(n_proj_d(d) + "/") })
   *        println
   *      }
   * }}}
   */
  def n_proj_d(d: Int) = {
    // no more than c many exist
    val c: BigInt = Combinatorics.comb(n_bits, d)
    val p = (rf * math.pow(base, n_bits - d)).toInt

    val np0 = if (d == n_bits) 1
    else if (d < mindim) 0
    else if (p < c) p
    else c.toInt

    // upper bound on number of cuboids of any given dimensionality
    if (np0 > 10000) 10000 else np0
  }

  println("Creating materialization strategy...")

  val projections = {
    val r = (for (d <- 0 to n_bits - 1) yield {
      val n_proj = n_proj_d(d)

      print(n_proj + "/")

      Util.collect_n[IndexedSeq[Int]](n_proj, () =>
        Util.collect_n[Int](d, () =>
          scala.util.Random.nextInt(n_bits)).toIndexedSeq.sorted)
    }
      ).flatten ++ Vector((0 to n_bits - 1))
    println("1")
    r
  }
  println("Total = " + projections.length)

}