package core.ds.settrie

import planning.NewProjectionMetaData

/**
 * A SetTrie representation of a Set of Lists of Integers.
 * Unsuccessful experiment, but interesting nonetheless
 * Allows to compute all intersections in a single DFS exploration
 */
class SetTrieIntersect(var capacity: Int) {
  val numFields = 5 //key, firstChild, nextSibling, cost, ±(cuboidID+1) if positive node is terminator, otherwise not terminator
  var nodes = new Array[Int](capacity * numFields)
  var nodeCount = 0
  val rootNodeValue = Vector(-1, -1, -1, 0, 0)

  def addNewNode(ns: Vector[Int]): Int = {
    assert(ns.length == numFields)
    val res = nodeCount * numFields
    if (nodeCount == capacity) {
      capacity = capacity << 1
      val newnodes = new Array[Int](capacity * numFields)
      java.lang.System.arraycopy(nodes, 0, newnodes, 0, res)
      nodes = newnodes
    }
    ns.indices.foreach(i => nodes(res + i) = ns(i))
    nodeCount += 1
    res
  }

  def findOrElseInsert(n: Int, k: Int, cost: Int, cuboidID: Int) = {
    if (nodeCount == 0) {
      addNewNode(rootNodeValue)
    }
    var cur = nodes(n + 1) //n.firstChild
    var prev = -1
    while (cur != -1 && nodes(cur) < k) { //cur.key
      prev = cur
      cur = nodes(cur + 2) //cur.nextSibling
    }
    if (cur != -1 && nodes(cur) == k) { //cur.key
      assert(nodes(cur + 3) <= cost) //check cost is cheaper
      cur
    } else {
      val newnode = addNewNode(Vector(k, -1, cur, cost, cuboidID))
      if (prev == -1) {
        nodes(n + 1) = newnode //n.firstChild
      } else {
        nodes(prev + 2) = newnode
      }
      newnode
    }
  }


  /**
   * Inserts a projection into the SetTrieIntersect
   * Note : Nodes need to be inserted in increasing size to avoid overwriting Node as terminator.
   *
   * @param s      The projection
   * @param size   The size of the projection (s.length())
   * @param id     The id of the projection
   * @param full_p The projection again, to be saved later in the terminator.
   * @param n      The node to start inserting from
   */
  def insert(s: Iterable[Int], size: Int, id: Int, full_p: Iterable[Int]): Unit = {
    var n = 0
    val iter = s.iterator
    //s is assumed to have distinct elements
    while (iter.hasNext) {
      val h = iter.next()
      val isTerm = !iter.hasNext
      n = findOrElseInsert(n, h, size, if (isTerm) id + 1 else -(id + 1))
    }
  }


}