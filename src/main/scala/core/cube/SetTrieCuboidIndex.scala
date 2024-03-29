package core.cube

import core.ds.settrie.{SetTrieForPrepare, SetTrieIntersect}
import core.materialization.MaterializationStrategy
import planning.NewProjectionMetaData
import util.Profiler

import java.io.{ObjectInputStream, ObjectOutputStream}
import scala.collection.mutable.ArrayBuffer

/**
 * Stores all projections in a SetTrie data structure. Uses the trie traversal for intersection in [[qproject]] as well
 * as to check for redundant cuboids in [[eliminateRedundant]]
 * @param n_bits Number of dimensions of the base cuboid and therefore, the data cube
 */
class SetTrieCuboidIndex(val trie: SetTrieIntersect, val projections: IndexedSeq[IndexedSeq[Int]], override val n_bits: Int) extends CuboidIndex(n_bits) {
  override val typeName: String = "SetTrie"
  override protected def saveToOOS(oos: ObjectOutputStream): Unit = ???
  override def qproject(query: IndexedSeq[Int], max_fetch_dim: Int): Seq[NewProjectionMetaData] = {
    val hm = collection.mutable.HashMap[Int, NewProjectionMetaData]()
    val queryLength = query.length
    val nodes = trie.nodes
    def intersectRec(qIdx: Int, n: Int, pDepth: Int, intersectQ: Int, intersectP: Vector[Int]): Unit = {
      if (nodes(n + 3) <= max_fetch_dim) { //cost <= max_fetch_dim
        if (intersectQ != 0 && (nodes(n + 4) > 0 )) { //n.cuboidId is positive , i.e. n is valid terminator.
          //We only want to check at the terminators, not every node. EXCEPT when query is fully processed -- we don't want to traverse till a valid terminator
          //Also, optimization for empty intersections, no need to check in hash map
          val res = hm.get(intersectQ)
          if (!res.isDefined || nodes(n + 3) < res.get.cuboidCost) {
            hm(intersectQ) = NewProjectionMetaData(intersectQ, math.abs(nodes(n + 4))-1, nodes(n + 3), intersectP) //convert encoded id to the correct value
          }
        }
        var child = nodes(n + 1) //n.firstChild
        var i = qIdx
        while (child != -1 && i < queryLength) {
          val qb = query(i)
          if (qb == nodes(child)) { //child.key == qb
            //There is intersection, advance query and recursively check for children
            intersectRec(i + 1, child, pDepth + 1, intersectQ + (1 << i), intersectP :+ pDepth)
            child = nodes(child + 2) //child.nextSibling
          } else if (qb > nodes(child)) {
            //No intersection, but recursively check for children without advancing query
            intersectRec(i, child, pDepth + 1, intersectQ, intersectP)
            child = nodes(child + 2) //child.nextSibling
          } else {
            //Advance query until we can check for intersection
            i += 1
          }
        }
        if(child != -1  && i == queryLength) {
          val res = hm.get(intersectQ)
          if (!res.isDefined || nodes(n + 3) < res.get.cuboidCost) {
            hm(intersectQ) = NewProjectionMetaData(intersectQ, math.abs(nodes(n + 4))-1, nodes(n + 3), intersectP) //convert encoded id to the correct value
          }
        }
      }
    }
    intersectRec(0, 0, 0, 0, Vector())
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
  override def length: Int = projections.length
  override def apply(idx: Int): IndexedSeq[Int] = projections(idx)
}

object SetTrieCuboidIndexFactory extends CuboidIndexFactory {
  override def buildFrom(m: MaterializationStrategy): CuboidIndex = {
    val minD = m.projections.head.length
    val trie = new SetTrieIntersect(m.projections.length * 2 * minD)
    m.projections.zipWithIndex.sortBy(res => res._1.size).foreach{res => trie.insert(res._1, res._1.size, res._2)}
    new SetTrieCuboidIndex(trie, m.projections, m.n_bits)
  }
  override def loadFromOIS(ois: ObjectInputStream): CuboidIndex = ???
}