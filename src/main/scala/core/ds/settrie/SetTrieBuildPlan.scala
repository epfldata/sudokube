package core.ds.settrie

class SetTrieBuildPlan(var capacity: Int = 16) {
  val numFields = 5 //key, firstChild, nextSibling, cost, id
  var nodes = new Array[Int](capacity * numFields)
  var nodeCount = 0

  var cheapest_id = 0
  var cheapest_cost = Int.MaxValue

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

  def findOrElseInsert(n: Int, k: Int, cost: Int, id: Int) = {
    var cur = nodes(n + 1) //n.firstChild
    var prev = -1
    while (cur != -1 && nodes(cur) < k) { //cur.key
      prev = cur
      cur = nodes(cur + 2) //cur.nextSibling
    }
    if (cur != -1 && nodes(cur) == k) { //cur.key
      if (nodes(cur + 3) > cost) { //update cost if cheaper
        nodes(cur + 3) = cost
        nodes(cur + 4) = id
      }
      cur
    } else {
      val newnode = addNewNode(Vector(k, -1, cur, cost, id))
      if (prev == -1) {
        nodes(n + 1) = newnode //n.firstChild
      } else {
        nodes(prev + 2) = newnode
      }
      newnode
    }
  }

  def insert(s: Iterable[Int], this_cost: Int, this_id: Int): Unit = {
    var n = 0
    if (nodeCount == 0)
      addNewNode(Vector(-1, -1, -1, this_cost, this_id))
    //s is assumed to have distinct elements
    s.foreach { h =>
      n = findOrElseInsert(n, h, this_cost, this_id)
    }
  }

  def extractCheapestSuperset(s: Iterable[Int], n: Int): Unit = {
    //print(" " + n)
    if (s.isEmpty) {
      cheapest_cost = nodes(n + 3)
      cheapest_id = nodes(n + 4)
    } else {
      val h = s.head
      var ckey = nodes(n)
      var child = nodes(n + 1)
      while (child != -1 && ckey <= h) {
        ckey = nodes(child)
        val nextSib = nodes(child + 2)
        if (nodes(child + 3) < cheapest_cost) {
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
