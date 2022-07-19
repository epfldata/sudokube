//package ch.epfl.data.sudokube
package core.materialization

import util._

@SerialVersionUID(2L)
/**
 * Describes what projections are materialized from a base cuboid containing n_bits dimensions
 * @param n_bits
 */
abstract class MaterializationScheme(val n_bits: Int) extends Serializable {
  /** the metadata describing each projection in this scheme. */
  val projections: IndexedSeq[IndexedSeq[Int]]

}


object MaterializationScheme {
  def only_base_cuboid(n_bits: Int) = new MaterializationScheme(n_bits) {
    override val projections = Vector(0 until n_bits)
  }

  def all_cuboids(n_bits: Int) = new MaterializationScheme(n_bits) {
    override val projections = (0 until 1 << n_bits).map(i => Bits.fromInt(i).toIndexedSeq.sorted)
  }

  def all_subsetsOf(n_bits: Int, q: Seq[Int]) = new MaterializationScheme(n_bits) {
    override val projections = {
      val idxes = q.toIndexedSeq
      (0 until 1 << q.length).map(i => Bits.fromInt(i).map(idxes).toIndexedSeq.sorted) :+ (0 until n_bits)
    }
  }
}

/** TODO: also support the construction from a previously stored
 * materialization scheme.
 */
case class PresetMaterializationScheme(
                                        _n_bits: Int,
                                        val projections: IndexedSeq[IndexedSeq[Int]]
                                      ) extends MaterializationScheme(_n_bits)



