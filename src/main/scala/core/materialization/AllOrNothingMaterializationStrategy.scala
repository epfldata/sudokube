package core.materialization

import frontend.schema.Schema2


class AllOrNothingMaterializationStrategy (val sch: Schema2, val maxDims: Int) extends MaterializationStrategy (sch.n_bits) {
  /** the metadata describing each projection in this strategy. */
  override val projections: IndexedSeq[IndexedSeq[Int]] = {
    val cols = sch.columnVector.map { c => c -> c.encoder.bits.size }.filter(_._2 > 0)
    import collection.immutable.BitSet
    def rec(selected: BitSet, remaining: Int, current: Int): Set[(Int, BitSet)] = {
      val choices = cols.indices.filter(i => !selected.contains(i) && cols(i)._2 <= remaining)
      choices.par.map(c => rec(selected + c, remaining - cols(c)._2, current + cols(c)._2)).fold(Set(current -> selected))(_ ++ _)
    }
    val dimSets = rec(BitSet(), maxDims, 0)
    val vec = dimSets.toVector.sortBy(_._1).filter(_._1 > 2)

      vec.map(_._2.toVector.map{i => cols(i)._1.encoder.bits}.reduce(_ ++ _).sorted) ++
      Vector(0 until n_bits)
  }
}
