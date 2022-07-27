package core.cube

import core.materialization.MaterializationStrategy
import planning.{NewProjectionMetaData, ProjectionMetaData}
import util.{BitUtils, Profiler}
import BitUtils._
import java.io.{ObjectInputStream, ObjectOutputStream}

/**
 * Stores all projections in an array. Uses a naive algorithm for intersection in [[qproject]]
 * @param n_bits Number of dimensions of the base cuboid and therefore, the data cube
 */
class ArrayCuboidIndex(val projections: IndexedSeq[IndexedSeq[Int]], override val n_bits: Int) extends CuboidIndex(n_bits) {
  override val typeName: String = "Array"
  override protected def saveToOOS(oos: ObjectOutputStream): Unit = {
    oos.writeInt(n_bits)
    oos.writeObject(projections)
  }
  override def qproject(query: IndexedSeq[Int], max_fetch_dim: Int): Seq[NewProjectionMetaData] = {
    val qBS = query.toSet
    val qIS = query.toIndexedSeq
    val qp0 = projections.indices.map { id =>
      val p = projections(id)
      val ab0 = p.toSet.intersect(qBS) // unnormalized
      val ab = qIS.indices.filter(i => ab0.contains(qIS(i))) // normalized
      val abInt = SetToInt(ab)
      val bitpos = mk_list_bitpos(p, qBS)
      NewProjectionMetaData(abInt, id, p.length, bitpos)
    }.filter(_.cuboidCost <= max_fetch_dim)

    //Find cheapest cuboid for every query projections
    val qp1: Seq[NewProjectionMetaData] =
      qp0.groupBy(_.queryIntersection).mapValues(l =>
        l.sortBy(_.cuboidCost).head // find cheapest: min mask.length
      ).toSeq.map(_._2)

    qp1
  }

  override def eliminateRedundant(cubs: Seq[NewProjectionMetaData], cheap_size: Int): Seq[NewProjectionMetaData] = {
    cubs.filter(x => !cubs.exists(y => y.dominates(x, cheap_size)))
  }
  override def length: Int = projections.length
  override def apply(idx: Int): IndexedSeq[Int] = projections.apply(idx)
}

object ArrayCuboidIndexFactory extends CuboidIndexFactory {
  override def buildFrom(m: MaterializationStrategy): CuboidIndex = new ArrayCuboidIndex(m.projections, m.n_bits)
  override def loadFromOIS(ois: ObjectInputStream): CuboidIndex = {
    val nbits = ois.readInt()
    val projections = ois.readObject().asInstanceOf[IndexedSeq[IndexedSeq[Int]]]
    new ArrayCuboidIndex(projections, nbits)
  }

}