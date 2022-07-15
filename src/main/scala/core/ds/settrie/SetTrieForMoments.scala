package core.ds.settrie

import util.Bits

//we assume every node has moment stored
@SerialVersionUID(1L)
class SetTrieForMoments(maxSize: Int = 1024) extends Serializable {
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

  def getNormalizedSubsetMoments(q: IndexedSeq[Int]): List[(Int, Double)] = {
    val qarray = q

    def rec(n: SetTrieForMomentsNode, qidx: Int, branch: Int): List[(Int, Double)] = {
      var result = List[(Int, Double)](branch -> n.moment)

      var childId = n.firstChild
      var i = qidx

      while (childId != -1 && i < qarray.length) {
        val qb = qarray(i)
        val child = nodes(childId)
        if (child.b == qb) {
          result = result ++ rec(child, i + 1, branch + (1 << i))
          childId = child.nextSibling
        } else if (child.b < qb) {
          childId = child.nextSibling
        } else {
          i = i + 1
        }
      }
      result
    }
    rec(nodes(0), 0, 0)
  }

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

  def findOrElseInsert(n: SetTrieForMomentsNode, v: Int, moment: Double) = {
    var cur = n.firstChild
    var child = if (cur != -1) nodes(cur) else null
    var prev: SetTrieForMomentsNode = null

    while (cur != -1 && child.b < v) {
      prev = child
      cur = child.nextSibling
      child = if (cur != -1) nodes(cur) else null
    }
    if (child != null && child.b == v) {
      child
    } else {
      val newnode = nodes(nodeCount)
      newnode.set(v, moment, cur)
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
class SetTrieForMomentsNode(var b: Int, var moment: Double, var firstChild: Int, var nextSibling: Int) extends Serializable {
  def set(bval: Int, mval: Double, sibVal: Int) = {
    b = bval
    moment = mval
    nextSibling = sibVal
  }

}
