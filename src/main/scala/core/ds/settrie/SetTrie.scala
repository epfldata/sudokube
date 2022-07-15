package core.ds.settrie

import util.Bits

import scala.collection.mutable.SortedSet

/*
 TODO: Optimization: Make SetTrie object oriented - have differnt types inherit common type
      Optimization : Instead of having binary dimensions as nodes, store cosmetic dimensions as nodes with prefixes stored internally inside nodes
 */
class SetTrie() {
  final val numFields = 3 //key, firstChild, nextSibling
  val nodes = collection.mutable.ArrayBuffer[Int]()
  var nodeCount = 0
  val rootNodeValue = Vector(-1, -1, -1)
  def addNewNode(ns: Vector[Int]): Int = {
    assert(ns.length == numFields)
    val res = nodeCount * numFields
    nodes ++= ns
    nodeCount += 1
    res
  }

  def findOrElseInsert(n: Int, k: Int) = {
    var cur = nodes(n + 1) //n.firstChild
    var prev = -1
    while (cur != -1 && nodes(cur) < k) { //cur.key
      prev = cur
      cur = nodes(cur + 2) //cur.nextSibling
    }
    if (cur != -1 && nodes(cur) == k) { //cur.key
      cur
    } else {
      val newnode = addNewNode(Vector(k, -1, cur))
        if (prev == -1) {
          nodes(n + 1) = newnode //n.firstChild
        } else {
          nodes(prev + 2) = newnode
        }
      newnode
    }
  }
  def insertInt(si: Int): Unit = {
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
        n = findOrElseInsert(n, h)
      }
      s >>= 1
      h += 1
    }
  }
  def insert(s: Iterable[Int]): Unit = {
    if(nodeCount == 0) {
      addNewNode(rootNodeValue)
    }
    var n = 0
    s.foreach { h =>
      n = findOrElseInsert(n, h)
    }

  }

  def existsSuperSetInt(si0: Int, h0: Int = 0, n: Int = 0): Boolean = {
    if(nodeCount == 0) false else if (si0 == 0) true else {
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
        found = existsSuperSetInt(newsi, newh, child)
        child = nodes(child + 2) //child.nextSibling
      }
      found
    }
  }
  def existsSuperSet(s: Iterable[Int], n: Int = 0): Boolean = {
    if(nodeCount == 0) false else if (s.isEmpty) true else {
      val h = s.head
      var found = false
      var child = nodes(n + 1) //n.firstChild
      var ckey = nodes(n) //n.key
      while (child != -1 && !found && ckey <= h) {
        ckey = nodes(child) //child.key
        val newS = if(ckey == h) s.tail else s
        found = existsSuperSet(newS, child)
        child = nodes(child + 2) //child.nextSibling
      }
      found
    }
  }
}
