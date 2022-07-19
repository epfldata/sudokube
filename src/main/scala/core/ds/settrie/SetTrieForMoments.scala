package core.ds.settrie

import util.Bits

/**
 * Variant of [[SetTrie]] used for storing and looking up moments associated with subsets
 */
@SerialVersionUID(1L)
class SetTrieForMoments(maxSize: Int = 1024) extends Serializable {
  //Not optimized like other variants due to Double
  val nodes = Array.fill(maxSize)(new SetTrieForMomentsNode(-1, Double.NaN, -1, -1))
  var nodeCount = 0

  //insert all moments of cuboid
  def insertAll(cuboid: IndexedSeq[Int], moments: Array[Double]) = {
    val cArray = cuboid.sorted
    moments.indices.foreach { i =>
      val ls = Bits.fromInt(i).map(x => cArray(x)).sorted
      //println(s"Insert $i@$ls = ${moments(i)} ")
      if (nodeCount < maxSize) insert(ls, moments(i))
    }
  }

  /**
   * Returns all subsets of q (encoded using Int relative to q) and the associated moments
   */
  def getNormalizedSubsetMoments(q: IndexedSeq[Int]): List[(Int, Double)] = {
    val qarray = q

    def rec(n: SetTrieForMomentsNode, qidx: Int, queryIntersection: Int): List[(Int, Double)] = {
      var result = List[(Int, Double)](queryIntersection -> n.moment)

      var childId = n.firstChild
      var i = qidx

      //Traverse children
      while (childId != -1 && i < qarray.length) {
        val qkey = qarray(i)
        val child = nodes(childId)
        /** Recursively process children. 3 cases
         *  child.key < qkey : find subsets of the same set in the subtree rooted at child
         *  child.key == qkey: find subsets of the set without the first element in the subtree rooted at child
         *      Also increment qIdx and update the queryIntersection to include the first element
         *  child.key > qkey : increment qIdx and retry the same child
         * */
        if (child.key == qkey) {
          result = result ++ rec(child, i + 1, queryIntersection + (1 << i))
          childId = child.nextSibling
        } else if (child.key < qkey) {
          childId = child.nextSibling
        } else {
          i = i + 1
        }
      }
      result
    }
    rec(nodes(0), 0, 0)
  }
  /** Adds a new set to the trie along with its associated moment */
  def insert(s: Iterable[Int], moment: Double): Unit = {
    if(nodeCount == 0 && s.isEmpty) {
      nodes(0).moment = moment
      nodeCount = 1
    }
    var n = nodes(0)
    val iter = s.iterator
    while(iter.hasNext) {
      val h = iter.next()
      if(!iter.hasNext || n.moment != 0.0) {
        n = findOrElseInsert(n, h, moment)
      }
    }
  }

  /**
   * Looks for a child with key [[k]] in the node represented by [[n]]. If no such child is found, a child is added
   * at the appropriate position among all children of n in the sorted order of keys.
   */
  def findOrElseInsert(n: SetTrieForMomentsNode, k: Int, moment: Double) = {
    var cur = n.firstChild
    var child = if (cur != -1) nodes(cur) else null
    var prev: SetTrieForMomentsNode = null

    while (cur != -1 && child.key < k) {
      prev = child
      cur = child.nextSibling
      child = if (cur != -1) nodes(cur) else null
    }
    if (child != null && child.key == k) {
      child
    } else {
      val newnode = nodes(nodeCount)
      newnode.set(k, moment, cur)
      if (prev == null)
        n.firstChild = nodeCount
      else
        prev.nextSibling = nodeCount
      nodeCount += 1
      newnode
    }
  }
}

@SerialVersionUID(2L)
class SetTrieForMomentsNode(var key: Int, var moment: Double, var firstChild: Int, var nextSibling: Int) extends Serializable {
  def set(bval: Int, mval: Double, sibVal: Int) = {
    key = bval
    moment = mval
    nextSibling = sibVal
  }

}
