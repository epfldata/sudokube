package core.cube

import core.materialization.MaterializationScheme
import planning.NewProjectionMetaData

abstract class CuboidIndex extends IndexedSeq[IndexedSeq[Int]] {
  def saveToFile(path: String): Unit
  //def merge(that: CuboidIndex): CuboidIndex

  /** Projection without filtering.
   *
   * We shift bits because otherwise the solver/optimizers can't handle them.
   * This id is the index in projections. Assuming that the same ordering
   * is used for the cuboids, it can be used to look up cuboids in the
   * data cube.
   *
   * This returns as many elements as there are in projections.
   * {{{
   *    assert(qproject(q).length == projections.length)
   * }}}
   */
  //no assumption on order
  def qproject(query: IndexedSeq[Int], max_fetch_dim: Int): Seq[NewProjectionMetaData]
  //maintain same order
  def eliminateRedundant(cubs: Seq[NewProjectionMetaData], cheap_size: Int):  Seq[NewProjectionMetaData]
  def prepare(query: IndexedSeq[Int], cheap_size: Int, max_fetch_dim: Int) = {
    val cubs = qproject(query, max_fetch_dim).sortBy(-_.queryIntersection)
    val cubs2 = eliminateRedundant(cubs, cheap_size)
    if(cheap_size == max_fetch_dim) cubs2 else cubs2.reverse  //biggest to smallest for batch mode, reverse for online mode
  }
}


trait CuboidIndexFactory {
  def buildFrom(m: MaterializationScheme) : CuboidIndex
  def loadFromFile(path: String): CuboidIndex
}
