//package ch.epfl.data.sudokube
package core.materialization

import util._

@SerialVersionUID(2L)
abstract class MaterializationScheme(val n_bits: Int) extends Serializable {
  /** the metadata describing each projection in this scheme. */
  val projections: IndexedSeq[List[Int]]

}


object MaterializationScheme {
  def only_base_cuboid(n_bits: Int) = new MaterializationScheme(n_bits) {
    override val projections: IndexedSeq[List[Int]] = Vector((0 until n_bits).toList)
  }

  def all_cuboids(n_bits: Int) = new MaterializationScheme(n_bits) {
    override val projections: IndexedSeq[List[Int]] = (0 until 1 << n_bits).map(i => Bits.fromInt(i).sorted)
  }

  def all_subsetsOf(n_bits: Int, q: Seq[Int]) = new MaterializationScheme(n_bits) {
    override val projections: IndexedSeq[List[Int]] = {
      val idxes = q.toIndexedSeq
      (0 until 1 << q.length).map(i => Bits.fromInt(i).map(idxes).sorted) :+ (0 until n_bits).toList
    }
  }
}

/** TODO: also support the construction from a previously stored
 * materialization scheme.
 */
case class PresetMaterializationScheme(
                                        _n_bits: Int,
                                        val projections: IndexedSeq[List[Int]]
                                      ) extends MaterializationScheme(_n_bits)



