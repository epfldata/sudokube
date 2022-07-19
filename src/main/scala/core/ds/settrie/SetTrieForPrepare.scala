package core.ds.settrie


/**
  A variant of [[SetTrie]] used in Prepare to remove redundant projections. A projection is redundant if there exists a
  superset that is either same cost or below cheap_size.
 */
class SetTrieForPrepare(var capacity: Int) {
  /**
   * Optimization for JVM. We want an array of structs to be continuous in memory. We encode a Node as [[numFields]]
   * consecutive integers.
   * Every node contains --
   * key: (element from set),
   * firstChild : index to first child of this node,
   * nextSibling:  index to next sibling within the parent node,
   * cost: cheapest cost for any cuboid in the subtree rooted at this node
   */
  final val numFields = 4 //key, firstChild, nextSibling, cost
  var nodes = new Array[Int](capacity * numFields)
  var nodeCount = 0
  val rootNodeValue = Vector(-1, -1, -1, 0)

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
   * at the appropriate position among all children of n in the sorted order of keys. Also updates the cost of
   * the current node if the cuboid being inserted along this path has a cheaper cost than the cheapest so far in the the
   * subtree rooted at this node
   */
  def findOrElseInsert(n: Int, k: Int, cost: Int) = {
    var cur = nodes(n + 1) //n.firstChild
    var prev = -1
    //Skip over children with smaller keys
    while (cur != -1 && nodes(cur) < k) { //cur.key
      prev = cur
      cur = nodes(cur + 2) //cur.nextSibling
    }
    //Child exists
    if (cur != -1 && nodes(cur) == k) { //cur.key
      //TODO: Perhaps we can remove the check?
      if(nodes(cur + 3) > cost) nodes(cur + 3) = cost  //update cost if cheaper
      cur
    } else {
      //Child does not exist, insert new one
      val newnode = addNewNode(Vector(k, -1, cur, cost))
      if (prev == -1) {  //no children so far
        nodes(n + 1) = newnode //n.firstChild
      } else {
        nodes(prev + 2) = newnode //prev.nextSibling
      }
      newnode
    }
  }
  /** Adds a new set to the trie with its associated cost. The set is encoded using integer whose binary notation contains 1s at indexes
   * corresponding to elements in the set */
  def insertInt(si: Int, cost: Int): Unit = {
    if(nodeCount == 0) {
      addNewNode(rootNodeValue)
    }
    assert(si >= 0)
    var s = si
    var n = 0
    var h = 0 //bit position
    while (s > 0) {
      val b = s & 1
      if (b != 0) {
        n = findOrElseInsert(n, h, cost) //if lsb is 1 then the element h exists in the set
      }
      s >>= 1
      h += 1
    }
  }
  /** Returns true if there exists a Set in the SetTrie that is a superset of the given List and is cheap.
   *  A superset is cheap if it is same cost as this set or its cost is below cheap_cost
   * @param si0 The set (encoded using Int) for which we want to check existence of cheap/cheaper superset
   * @param cheapCost Max(the cost of the set, cheap_size)
   * @param h0 Current bit position within the set si0
   * @param n The node at which the search starts
   * @return Whether there exists a cheap/cheaper superset
   */
  def existsCheapSuperSetInt(si0: Int, cheapCost: Int, h0: Int = 0, n: Int = 0): Boolean = {
    if (nodeCount == 0 || nodes(n + 3) > cheapCost) {  //this node has cost greater than s_cost. All supersets will have at least this much cost
      false
    } else if(si0 == 0) {
      true //the presence of this node indicates there exists some cheaper node along the path
    } else {
      var h = h0
      var si = si0
      var b = si & 1
      //Traverse the set until an element is found (lsb is 1)
      while (b == 0) {
        h += 1
        si >>= 1
        b = si & 1
      }
      //h contains the first element of the set now
      var found = false
      var child = nodes(n + 1) //n.firstChild
      var ckey = nodes(n) //n.key
      /**
       * Recursively check for superset from children. There are 3 cases for the key stored in child (ckey)
       * ckey < h : look for the supersets of the same set
       * ckey == h : look for superset of the set after excluding h
       * ckey > h : no need to look further. h is missing, so no super set can exist
       */
      while (child != -1 && !found && ckey <= h) {
        ckey = nodes(child) //child.b
        val (newsi, newh) = if(ckey == h)  (si >> 1, h + 1) else (si, h)
        found = existsCheapSuperSetInt(newsi, cheapCost, newh, child)
        child = nodes(child + 2) //child.nextSibling
      }
      found
    }
  }
}

