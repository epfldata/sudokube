package core.ds.settrie

import planning.NewProjectionMetaData

/**
  Variant of [[SetTrie]] to be used a [[core.cube.CuboidIndex]] for storing projections and retrieving the ones relevant
 to some query.
 */
class SetTrieIntersect(var capacity: Int) {
/**
 * Optimization for JVM. We want an array of structs to be continuous in memory. We encode a Node as [[numFields]]
 * consecutive integers.
 * Every node contains --
 * key: (element from set),
 * firstChild : index to first child of this node,
 * nextSibling:  index to next sibling within the parent node,
 * cost: cheapest cost for any cuboid in the subtree rooted at this node,
 * id: stores ±(cuboidID+1) of the cuboid in the subtree with cheapeast cost. If positive, node is a terminator, otherwise not terminator.
 */
  val numFields = 5 //key, firstChild, nextSibling, cost, id
  var nodes = new Array[Int](capacity * numFields)
  var nodeCount = 0
  val rootNodeValue = Vector(-1, -1, -1, 0, 0)

  /** Adds a new node at the end of the array, resizes the array if required
   * @param ns  Values of the node encoded as integers. Must be of length [[numFields]] */
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


  /**
   * Looks for a child with key [[k]] in the node represented by [[n]]. If no such child is found, a child is added
   * at the appropriate position among all children of n in the sorted order of keys. Also updates the cost and id of
   * the current node if the cuboid being inserted along this path has a cheaper cost than the cheapest so far in the the
   * subtree rooted at this node
   */

  def findOrElseInsert(n: Int, k: Int, cost: Int, cuboidID: Int) = {
    if (nodeCount == 0) {
      addNewNode(rootNodeValue)
    }
    var cur = nodes(n + 1) //n.firstChild
    var prev = -1
    //Skip over children with smaller keys
    while (cur != -1 && nodes(cur) < k) { //cur.key
      prev = cur
      cur = nodes(cur + 2) //cur.nextSibling
    }
    //Child exists
    if (cur != -1 && nodes(cur) == k) { //cur.key
      assert(nodes(cur + 3) <= cost) //check cost is cheaper
      cur
    } else {
      //Child does not exist, insert new one
      val newnode = addNewNode(Vector(k, -1, cur, cost, cuboidID))
      if (prev == -1) { //no children so far
        nodes(n + 1) = newnode //n.firstChild
      } else {
        nodes(prev + 2) = newnode //prev.nextSibling
      }
      newnode
    }
  }


  /**
   * Inserts a set with its id and cost
   * Note :  Sets need to be inserted in increasing size to avoid overwriting Node as terminator.
   *
   * @param s      The projection
   * @param size   The size of the projection (s.length())
   * @param id     The id of the projection
   */
  def insert(s: Iterable[Int], size: Int, id: Int): Unit = {
    var n = 0
    val iter = s.iterator
    //s is assumed to have distinct elements
    while (iter.hasNext) {
      val h = iter.next()
      val isTerm = !iter.hasNext
      //using id to indicate whether the node is terminator or not
      n = findOrElseInsert(n, h, size, if (isTerm) id + 1 else -(id + 1))
    }
  }


}