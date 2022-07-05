package core.materialization

import core.{SetTrieIntersect, SetTrieOnline}
import planning.ProjectionMetaData
import util.Bits

/**
 * Wrapper around materialization scheme that builds a Set-Trie index on the list of materialized cuboids
 * @param m Materialization Scheme
 */
case class SetTrieMaterializationScheme(m: MaterializationScheme) extends MaterializationScheme(m.n_bits) {
  /** the metadata describing each projection in this scheme. */
  override val projections: IndexedSeq[List[Int]] = m.projections

  val proj_trie = {
    val trie = new SetTrieIntersect()
    projections.zipWithIndex.sortBy(res => res._1.size).foreach(res => trie.insert(res._1, res._1.size, res._2, res._1))
    trie
  }

}
