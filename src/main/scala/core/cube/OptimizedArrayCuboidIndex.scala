package core.cube

import core.ds.settrie.SetTrieForPrepare
import core.materialization.MaterializationStrategy
import planning.NewProjectionMetaData
import util.Profiler

import java.io.ObjectInputStream
import scala.collection.mutable.ArrayBuffer

/**
 * Stores all projections in an array. Uses an optimized algorithm for intersection in [[qproject]]
 * @param n_bits Number of dimensions of the base cuboid and therefore, the data cube
 */
class OptimizedArrayCuboidIndex(override val projections: IndexedSeq[IndexedSeq[Int]], override val n_bits: Int) extends ArrayCuboidIndex(projections, n_bits) {
  override val typeName: String = "OptimizedArray"
  def intersect(x: IndexedSeq[Int], y: IndexedSeq[Int]): (Int, IndexedSeq[Int]) = {
    var xid = 0
    var yid = 0

    val xMax = x.length
    val yMax = y.length

    var intersectionX = 0
    val intersectionY = new collection.mutable.ArrayBuffer[Int](xMax min yMax)

    while (xid < xMax && yid < yMax) {
      val xVal = x(xid)
      val yVal = y(yid)
      if (xVal > yVal) {
        yid += 1
      } else if (xVal < yVal) {
        xid += 1
      }
      else {
        intersectionX += (1 << xid)
        intersectionY += yid
        xid += 1
        yid += 1
      }
    }
    intersectionX -> intersectionY
  }

  override def qproject(query: IndexedSeq[Int], max_fetch_dim: Int): Seq[NewProjectionMetaData] = {

    val hm =  collection.mutable.Map[Int, NewProjectionMetaData]()
    projections.indices.foreach { id =>
      val p = projections(id)
      val pSize = p.size
      if (pSize <= max_fetch_dim) {
        val (qposInt, cpos) = intersect(query, p)
        val hmres = hm.get(qposInt)
        if (!hmres.isDefined || hmres.get.cuboidCost > pSize) {
          hm(qposInt) = NewProjectionMetaData(qposInt, id, pSize, cpos)
        }
      }
    }
    hm.values.toSeq
  }

  override def eliminateRedundant(cubs: Seq[NewProjectionMetaData], cheap_size: Int): Seq[NewProjectionMetaData] = {
    val result = new ArrayBuffer[NewProjectionMetaData]()
    val trie = new SetTrieForPrepare(cubs.length * cubs.head.cuboidCost)
    //cubs must processed in the order supersets first and then subsets
    cubs.foreach { p =>
      //Add to result if there does not exist a cheap super set.
      if (!trie.existsCheapSuperSetInt(p.queryIntersection, p.cuboidCost max cheap_size)) {
        trie.insertInt(p.queryIntersection, p.cuboidCost)
        result += p
      }
    }
    result
  }
}

object OptimizedArrayCuboidIndexFactory extends CuboidIndexFactory {
  override def buildFrom(m: MaterializationStrategy): CuboidIndex = new OptimizedArrayCuboidIndex(m.projections, m. n_bits)
  override def loadFromOIS(ois: ObjectInputStream): CuboidIndex = {
    val nbits = ois.readInt()
    val projections = ois.readObject().asInstanceOf[IndexedSeq[IndexedSeq[Int]]]
    new OptimizedArrayCuboidIndex(projections, nbits)
  }
}