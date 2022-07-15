package core.ds.settrie

import scala.collection.mutable.SortedSet

/**
 * A SetTrie representation of a Set of Lists of Integers.
 * Used for prepare online mode, to check for domination.
 */
class SetTrieForPrepare(var capacity: Int) {
  final val numFields = 4 //key, firstChild, nextSibling, cost
  var nodes = new Array[Int](capacity * numFields)
  var nodeCount = 0
  val rootNodeValue = Vector(-1, -1, -1, 0)

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

  def findOrElseInsert(n: Int, k: Int, cost: Int) = {
    var cur = nodes(n + 1) //n.firstChild
    var prev = -1
    while (cur != -1 && nodes(cur) < k) { //cur.key
      prev = cur
      cur = nodes(cur + 2) //cur.nextSibling
    }
    if (cur != -1 && nodes(cur) == k) { //cur.key
      //TODO: Perhaps we can remove the check?
      if(nodes(cur + 3) > cost) nodes(cur + 3) = cost  //update cost if cheaper
      cur
    } else {
      val newnode = addNewNode(Vector(k, -1, cur, cost))
      if (prev == -1) {
        nodes(n + 1) = newnode //n.firstChild
      } else {
        nodes(prev + 2) = newnode
      }
      newnode
    }
  }

  def insertInt(si: Int, cost: Int): Unit = {
    if(nodeCount == 0) {
      addNewNode(rootNodeValue)
    }
    assert(si >= 0)
    var s = si
    var n = 0
    var h = 0
    while (s > 0) {
      val b = s & 1
      if (b != 0) {
        n = findOrElseInsert(n, h, cost)
      }
      s >>= 1
      h += 1
    }
  }
  /** Returns true if there exists a Set in the SetTrie that is a superset of the given List and is cheaper
   *
   * @param s The list for which we want to check existence of cheap/cheaper superset
   * @param cost The cost of the list, usually it's length
   * @param cheap_size The size under which any List is considered cheap
   * @param n The node at which the search starts
   * @return Whether there exists a cheap/cheaper superset
   */
  def existsCheapSuperSetInt(si0: Int, cost: Int, h0: Int = 0, n: Int = 0): Boolean = {
    if (nodeCount == 0 || nodes(n + 3) > cost) {  //this node has cost greater than s_cost. All supersets will have at least this much cost
      false
    } else if(si0 == 0) {
      true
    } else {
      var h = h0
      var si = si0
      var b = si & 1
      while (b == 0) {
        h += 1
        si >>= 1
        b = si & 1
      }
      var found = false
      var child = nodes(n + 1) //n.firstChild
      var ckey = nodes(n) //n.key
      while (child != -1 && !found && ckey <= h) {
        ckey = nodes(child) //child.b
        val (newsi, newh) = if(ckey == h)  (si >> 1, h + 1) else (si, h)
        found = existsCheapSuperSetInt(newsi, cost, newh, child)
        child = nodes(child + 2) //child.nextSibling
      }
      found
    }
  }
}

