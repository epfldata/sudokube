package core.ds.settrie

/**
 Variant of [[SetTrie]] used for finding cheapeast superset while creating build plans for the Data Cube
 */
class SetTrieBuildPlan(var capacity: Int = 16) {
  /**
   * Optimization for JVM. We want an array of structs to be continuous in memory. We encode a Node as [[numFields]]
   * consecutive integers.
   * Every node contains --
   * key: (element from set),
   * firstChild : index to first child of this node,
   * nextSibling:  index to next sibling within the parent node,
   * cost: cheapest cost for any cuboid in the subtree rooted at this node,
   * id: id of the cuboid in the subtree with cheapeast cost
   *
   */
  val numFields = 5 //key, firstChild, nextSibling, cost, id
  var nodes = new Array[Int](capacity * numFields)
  var nodeCount = 0

  var cheapest_id = 0
  var cheapest_cost = Int.MaxValue

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
  def findOrElseInsert(n: Int, k: Int, cost: Int, id: Int) = {
    var cur = nodes(n + 1) //n.firstChild
    var prev = -1
    //Skip over children with smaller keys
    while (cur != -1 && nodes(cur) < k) { //cur.key
      prev = cur
      cur = nodes(cur + 2) //cur.nextSibling
    }
    //Child exists
    if (cur != -1 && nodes(cur) == k) { //cur.key
      if (nodes(cur + 3) > cost) { //update cost if cheaper
        nodes(cur + 3) = cost
        nodes(cur + 4) = id
      }
      cur
    } else {
      //Child does not exist, insert new one
      val newnode = addNewNode(Vector(k, -1, cur, cost, id))
      if (prev == -1) { //no children so far
        nodes(n + 1) = newnode //n.firstChild
      } else {
        nodes(prev + 2) = newnode //prev.nextSibling
      }
      newnode
    }
  }

  /** Adds a new set to the trie with associated cost and id */
  def insert(s: Iterable[Int], this_cost: Int, this_id: Int): Unit = {
    var n = 0
    if (nodeCount == 0)
      addNewNode(Vector(-1, -1, -1, this_cost, this_id))
    //s is assumed to have distinct elements
    s.foreach { h =>
      n = findOrElseInsert(n, h, this_cost, this_id)
    }
  }
  /** Finds the cheapest superset in the trie rooted at [[n]] for a given set. There will always exist one because the
   * set containing all elements is added first
   * */
  def extractCheapestSuperset(s: Iterable[Int], n: Int): Unit = {
    /**
     * If s is empty, we don't need to look further in the tree. The current node stores the id and cost
     * of the cheapest superset
     */
    if (s.isEmpty) {
      cheapest_cost = nodes(n + 3)
      cheapest_id = nodes(n + 4)
    } else {
      val h = s.head
      var ckey = nodes(n)
      var child = nodes(n + 1)
      /**
       * Recursively check for superset from children. There are 3 cases for the key stored in child (ckey)
       * ckey < h : look for the supersets of the same set only if it is in the path of the cheapest set in the subtree
       * ckey == h : look for superset of the set after excluding h only if it is in the path of the cheapest set in the subtree
       * ckey > h : no need to look further. h is missing, so no super set can exist
       */
      while (child != -1 && ckey <= h) {
        ckey = nodes(child) //child.key
        val nextSib = nodes(child + 2) //child.nextSibling
        if (nodes(child + 3) < cheapest_cost) { //child.cost
          val newS = if (ckey == h) s.tail else s
          extractCheapestSuperset(newS, child)
        }
        child = nextSib
      }
    }
  }

  def getCheapestSuperset(s: Iterable[Int]): Int = {
    extractCheapestSuperset(s, 0)
    val ret = cheapest_id
    cheapest_id = 0
    cheapest_cost = Int.MaxValue
    ret
  }


}
