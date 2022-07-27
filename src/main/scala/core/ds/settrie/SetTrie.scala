package core.ds.settrie

import util.BitUtils

import scala.collection.mutable.SortedSet

/**
 * Basic SetTrie implementation following the ideas of [[https://journals.plos.org/plosone/article?id=10.1371/journal.pone.0245122]]
 * TODO: Optimization: Make SetTrie object oriented - have differnt types inherit common type
 * TODO: Optimization : Instead of having binary dimensions as nodes, store cosmetic dimensions as nodes with prefixes stored internally inside nodes
 * @param Initial value to the number of nodes. This value is upper bounded by ( number of sets * maximum set length).
 * Providing an accurate value avoids costly memory reallocations
*/
class SetTrie(var capacity: Int) {

  /**
   * Optimization for JVM. We want an array of structs to be continuous in memory. We encode a Node as [[numFields]]
   * consecutive integers.
   * Every node contains a key (element from set), firstChild (index to first child of this node), nextSibling (index to
   * next sibling within the parent node)
   */
  final val numFields = 3 //key, firstChild, nextSibling
  var nodes = new Array[Int](capacity * numFields)
  var nodeCount = 0
  val rootNodeValue = Vector(-1, -1, -1)

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
   * at the appropriate position among all children of n in the sorted order of keys.
   */
  def findOrElseInsert(n: Int, k: Int) = {
    var cur = nodes(n + 1) //n.firstChild
    var prev = -1
    //Skip over children with smaller keys
    while (cur != -1 && nodes(cur) < k) { //cur.key
      prev = cur
      cur = nodes(cur + 2) //cur.nextSibling
    }
    //Child exists
    if (cur != -1 && nodes(cur) == k) { //cur.key
      cur
    } else {
      //Child does not exist, insert new one
      val newnode = addNewNode(Vector(k, -1, cur))
      if (prev == -1) { //no children so far
        nodes(n + 1) = newnode //n.firstChild
      } else {
        nodes(prev + 2) = newnode //prev.nextSibling
      }
      newnode
    }
  }
  /** Adds a new set to the trie. The set is encoded using integer whose binary notation contains 1s at indexes
   * corresponding to elements in the set */
  def insertInt(si: Int): Unit = {
    if (nodeCount == 0) {
      addNewNode(rootNodeValue)
    }
    assert(si >= 0)
    var s = si
    var n = 0
    var h = 0 //bit position
    //Travese the set
    while (s > 0) {
      val b = s & 1
      if (b != 0) { //if lsb is 1 then the element h exists in the set
        n = findOrElseInsert(n, h)
      }
      s >>= 1
      h += 1
    }
  }

  /** Adds a new set to the trie */
  def insert(s: Iterable[Int]): Unit = {
    if (nodeCount == 0) {
      addNewNode(rootNodeValue)
    }
    var n = 0
    s.foreach { h =>
      n = findOrElseInsert(n, h)
    }

  }

  /** Checks whether there exists a superset in the trie rooted at [[n]] for a given set. The given set is encoded using an integer */
  def existsSuperSetInt(si0: Int, h0: Int = 0, n: Int = 0): Boolean = {
    if (nodeCount == 0) false else if (si0 == 0) true else {
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
       *Recursively check for superset from children. There are 3 cases for the key stored in child (ckey)
       * ckey < h : look for the supersets of the same set
       * ckey == h : look for superset of the set after excluding h
       * ckey > h : no need to look further. h is missing, so no super set can exist
       */
      while (child != -1 && !found && ckey <= h) {
        ckey = nodes(child) //child.b
        val (newsi, newh) = if (ckey == h) (si >> 1, h + 1) else (si, h)
        found = existsSuperSetInt(newsi, newh, child)
        child = nodes(child + 2) //child.nextSibling
      }
      found
    }
  }
  /** Checks whether there exists a superset in the trie rooted at [[n]] for a given set. */
  def existsSuperSet(s: Iterable[Int], n: Int = 0): Boolean = {
    if (nodeCount == 0) false else if (s.isEmpty) true else {
      val h = s.head
      var found = false
      var child = nodes(n + 1) //n.firstChild
      var ckey = nodes(n) //n.key

      /**
       *Recursively check for superset from children. There are 3 cases for the key stored in child (ckey)
       * ckey < h : look for the supersets of the same set
       * ckey == h : look for superset of the set after excluding h
       * ckey > h : no need to look further. h is missing, so no super set can exist
       *
       */
      while (child != -1 && !found && ckey <= h) {
        ckey = nodes(child) //child.key
        val newS = if (ckey == h) s.tail else s
        found = existsSuperSet(newS, child)
        child = nodes(child + 2) //child.nextSibling
      }
      found
    }
  }
}
