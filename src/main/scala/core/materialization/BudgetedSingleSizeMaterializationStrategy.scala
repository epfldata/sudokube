package core.materialization

import combinatorics.Combinatorics
import util.Util

/**
 * Materialization strategy that spends the whole budget in one dimension.
 * The cuboids to materialize are selected with replacement.
 *
 * @param _n_bits Number of bits of the base cuboid.
 * @param d0 Sparsity of the base cuboid (contains pow(2, d0) non-zero entries).
 * @param k Dimensionality of the cuboids to materialize.
 * @param b Budget.
 */
case class BudgetedSingleSizeMaterializationStrategy(_n_bits: Int, d0: Int, k: Int, b: Double) extends MaterializationStrategy(_n_bits) {
  /** the metadata describing each projection in this strategy. */
  override val projections: IndexedSeq[IndexedSeq[Int]] = {
    val N0 = (b * (math.pow(2, d0) / math.pow(2, k + 3)) * ((_n_bits + 64.0) / 8.0).ceil).floor
    val c = Combinatorics.comb(n_bits, k).toDouble
    val n_proj = (c min N0).toInt
    val cubD = Util.collect_n[IndexedSeq[Int]](n_proj, () =>
      Util.collect_n[Int](k, () =>
        scala.util.Random.nextInt(n_bits)).toIndexedSeq.sorted).toVector
    cubD ++ Vector(0 until n_bits)
  }
}
