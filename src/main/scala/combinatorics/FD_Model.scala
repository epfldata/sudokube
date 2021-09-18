//package ch.epfl.data.sudokube
package combinatorics
import util._


/** Storage consumption model with the assumption that the
    base data consists of 2^m tuples and there
    are functional dependencies so that m dimensions determine all n
    dimensions.
    Here m is NOT the dimensionality of the query.
*/
object FD_Model {
  import Combinatorics._

  /** Storage consumption of layer k (k-dimensional cuboids) under
      the assumption that each cuboid's size is 2^l, where l is the number
      of dimensions of interest to us.

      @param k is the layer from the bottom (= dimensions of the cuboids)
  */
  def cube_layer_storage(n: Int, m: Int, k: Int) : BigInt =
    (0 to k).map(l => lcomb(n, m, k, l) * Big.pow2(l)).sum


  //SBJ: Same as comb(n, k)
  def cube_layer_n_cuboids(n: Int, m: Int, k: Int) : BigInt =
    (0 to k).map(l => lcomb(n, m, k, l)).sum

  /** in expectation, a set of size k will have a projection of this size */
  def avg_u(d: Int, s: Int, k: Int) : BigInt =
    cube_layer_storage(d, s, k) / cube_layer_n_cuboids(d, s, k)

  /** the crazy cost of materializing all projections. */
  def complete_cube_storage_wlabels(d: Int, s: Int) : BigInt =
    (0 to d).map(k => cube_layer_storage(d, s, k)).sum

  /** estimated storage overhead. */
  def est(n: Int, s: Int, rf: Double, base: Double, mindim: Int = 0
  ) : BigDecimal= {
    val m = core.RandomizedMaterializationScheme(n, rf, base, mindim)
    val total = (for(d <- 0 to n) yield { m.n_proj_d(d) * avg_u(n, s, d) }).sum
    BigDecimal(total) / Big.pow(2, s)
  }

  /** estimated storage overhead. */
  def est_write(n: Int, s: Int, rf: Double, base: Double, mindim: Int = 0
  ) : BigDecimal= {
    import java.io._
    val pw = new PrintWriter(new File("fd_storage_"
      + n + "_" + rf + "_" + base + "_" + mindim + "_" + s  + ".csv" ))

    pw.write("#Dimensions\t#Cuboids\tAvg. Cuboid Size\tStorage Consumption\n")

    val m = core.RandomizedMaterializationScheme(n, rf, base, mindim)
    val total = (for(d <- 0 to n) yield {
      val n_proj = m.n_proj_d(d)
      val a = avg_u(n, s, d)
      val spc = a * n_proj
      pw.write(d + "\t" + n_proj + "\t" + a + "\t" + spc + "\n")
      print(n_proj + "/")
      spc
    }).sum

    println
    pw.close
    BigDecimal(total) / Big.pow(2, s)
  }
} // end object fd_model


