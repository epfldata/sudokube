package core.cube

import core.materialization.MaterializationScheme
import planning.{NewProjectionMetaData, ProjectionMetaData}
import util.{Bits, Profiler}

class ArrayCuboidIndex(val projections: IndexedSeq[IndexedSeq[Int]]) extends CuboidIndex {
  override def saveToFile(path: String): Unit = ???

  override def qproject(query: IndexedSeq[Int], max_fetch_dim: Int): Seq[NewProjectionMetaData] = Profiler("ACI qP"){
    val qBS = query.toSet
    val qIS = query.toIndexedSeq
    val qp0 = projections.indices.map { id =>
      val p = projections(id)
      val ab0 = p.toSet.intersect(qBS) // unnormalized
      val ab = qIS.indices.filter(i => ab0.contains(qIS(i))) // normalized
      val abInt = Bits.toInt(ab)
      val bitpos = Bits.mk_list_bitpos(p, qBS)
      NewProjectionMetaData(abInt, id, p.length, bitpos)
    }.filter(_.cuboidCost <= max_fetch_dim)

    val qp1: Seq[NewProjectionMetaData] =
      qp0.groupBy(_.queryIntersection).mapValues(l =>
        l.sortBy(_.cuboidCost).head // find cheapest: min mask.length
      ).toSeq.map(_._2)

    qp1
  }

  override def eliminateRedundant(cubs: Seq[NewProjectionMetaData], cheap_size: Int): Seq[NewProjectionMetaData] = Profiler("ACI eR"){
    cubs.filter(x => !cubs.exists(y => y.dominates(x, cheap_size)))
  }
  override def length: Int = projections.length
  override def apply(idx: Int): IndexedSeq[Int] = projections.apply(idx)
}

object ArrayCuboidIndexFactory extends CuboidIndexFactory {
  override def buildFrom(m: MaterializationScheme): CuboidIndex = new ArrayCuboidIndex(m.projections)
  override def loadFromFile(path: String): CuboidIndex = ???
}