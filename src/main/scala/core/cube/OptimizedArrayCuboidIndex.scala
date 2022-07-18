package core.cube

import core.materialization.MaterializationScheme
import planning.NewProjectionMetaData
import util.Profiler

class OptimizedArrayCuboidIndex(override val projections: IndexedSeq[IndexedSeq[Int]]) extends ArrayCuboidIndex(projections) {
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

  override def qproject(query: IndexedSeq[Int], max_fetch_dim: Int): Seq[NewProjectionMetaData] = Profiler("OACI qp"){

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

  override def eliminateRedundant(cubs: Seq[NewProjectionMetaData], cheap_size: Int): Seq[NewProjectionMetaData] = Profiler("OACI eR"){
    cubs.filter(x => !cubs.exists(y => y.dominates(x, cheap_size)))
  }
}

object OptimizedArrayCuboidIndexFactory extends CuboidIndexFactory {
  override def buildFrom(m: MaterializationScheme): CuboidIndex = new OptimizedArrayCuboidIndex(m.projections)
  override def loadFromFile(path: String): CuboidIndex = ???
}