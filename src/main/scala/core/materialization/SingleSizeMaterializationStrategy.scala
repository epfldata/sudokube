package core.materialization

import combinatorics.Combinatorics
import util.Util

case class SingleSizeMaterializationStrategy(_n_bits: Int, d: Int, logN: Int) extends MaterializationStrategy(_n_bits) {
  /** the metadata describing each projection in this strategy. */
  override val projections: IndexedSeq[IndexedSeq[Int]] = {
    val N0 = 1 << logN
    val c = Combinatorics.comb(n_bits, d).toDouble
    val n_proj = (c min N0).toInt
    val cubD = Util.collect_n[IndexedSeq[Int]](n_proj, () =>
      Util.collect_n[Int](d, () =>
        scala.util.Random.nextInt(n_bits)).toIndexedSeq.sorted).toVector
    cubD ++ Vector(0 until n_bits)
  }
}
