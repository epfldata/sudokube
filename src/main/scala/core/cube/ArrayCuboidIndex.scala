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
      val mask = Bits.mk_list_mask(p, qBS)
      ProjectionMetaData(ab, ab0, mask, id)
    }.filter(_.mask.length <= max_fetch_dim)

    val qp1: Seq[ProjectionMetaData] =
      qp0.groupBy(_.accessible_bits).mapValues(l =>
        l.sortBy(_.mask.length).head // find cheapest: min mask.length
      ).toSeq.map(_._2)

    qp1.map { case ProjectionMetaData(accessible_bits, accessible_bits0, mask, id) =>
      val qposInt = Bits.toInt(accessible_bits)
      val maskpos = mask.indices.filter(i => mask(i) == 1)
      NewProjectionMetaData(qposInt, id, mask.length, maskpos)
    }
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