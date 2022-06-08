package core

import collection.mutable.SortedSet

import util.Bits
//we assume every node has moment stored
@SerialVersionUID(1L)
class SetTrieForMoments() extends  Serializable {
  var root: Node = null
  var count = 0
  //insert all moments of cuboid
  def insertAll(cuboid: List[Int], moments: Array[Double]) = {
    val cArray = cuboid.sorted.toArray
    moments.indices.foreach { i =>
      val ls = Bits.fromInt(i).map(x => cArray(x)).sorted
      //println(s"Insert $i@$ls = ${moments(i)} ")
      insert(ls, moments(i))
    }
  }

  def getNormalizedSubsetMoments(q: List[Int]): List[(Int, Double)] = {
    val qarray = q.toArray

    def rec(n: Node, qidx: Int, branch: Int): List[(Int, Double)] = {
      var result = List[(Int, Double)](branch -> n.moment)

      var child = n.firstChild
      var i = qidx


      while (child != null && i < qarray.length) {
        val qb = qarray(i)
        if (child.b == qb) {
          result = result ++ rec(child, i + 1, branch + (1 << i))
          child = child.nextSibling
        } else if (child.b < qb) {
          child = child.nextSibling
        } else {
          i = i + 1
        }
      }
      result
    }

    rec(root, 0, 0)
  }

  def insert(s: List[Int], moment: Double, n: Node = root): Unit = s match {
    //s is assumed to have distinct elements
    //all subsets assumed to be inserted before some set
    case Nil if root == null =>
      root = new Node(-1, moment, null, null)
      count = 1
    case Nil => ()
    case h :: t =>
      if(t.isEmpty || n.moment != 0.0) {  //do not insert child 0 moments
        val (inserted, c) = n.findOrElseInsert(h, moment)
        //will always find node except for last
        if (inserted) count = count + 1
        if (t.isEmpty) assert(c.moment == moment) else
          insert(t, moment, c)
      }
  }

  @SerialVersionUID(2L)
  class Node(val b: Int, val moment: Double, var firstChild: Node, var nextSibling: Node) extends Serializable {
    def findOrElseInsert(v: Int, moment: Double) = {
     var child = firstChild
      var prev: Node = null

      while(child != null && child.b < v) {
        prev = child
        child = child.nextSibling
      }
      if(child != null && child.b == v) {
        (false, child)
      } else {
        val nc = new Node(v, moment, null, child)
        if(prev == null)
          firstChild = nc
        else
          prev.nextSibling = nc
        (true, nc)
      }
    }
  }
}

class SetTrie() {
  val root =  Node(-1)

  def insert(s: List[Int], n: Node = root): Unit = s match {
    //s is assumed to have distinct elements
    case Nil => ()
    case h :: t =>
      val c = n.findOrElseInsert(h)
      insert(t, c)
  }


  def existsSuperSet(s: List[Int], n: Node = root): Boolean= s match {
    case Nil => true
    case h :: t =>
      var found = false
      val child = n.children.iterator
      var ce = n.b
      while (child.hasNext && !found && ce <= h) {
       val cn = child.next()
        ce = cn.b
       if(ce < h) {
         found = existsSuperSet(s, cn)
       } else if(ce == h) {
         found = existsSuperSet(t, cn)
       } else
         ()
      }
      found
  }

  class Node(val b: Int, val children: SortedSet[Node]) {
    def findOrElseInsert(v: Int) = {
      val c = children.find(_.b == v)
      if (c.isDefined)
        c.get
      else {
        val nc = new Node(v, SortedSet())
        children += nc
        nc
      }
    }
  }

  object Node {
    implicit def order: Ordering[Node] = Ordering.by(_.b)
    def apply(i: Int) = new Node(i, SortedSet())
  }
}

object SetTrie {
  def main(args: Array[String]): Unit = {
    val trie = new SetTrie
    trie.insert(List(1, 2, 4))
    trie.insert(List(1, 3, 5))
    trie.insert(List(1, 4))
    trie.insert(List(2, 3, 5))
    trie.insert(List(2, 4))

    println(trie.existsSuperSet(List(3, 5)))
    println(trie.existsSuperSet(List(2, 6)))
    println(trie.existsSuperSet(List(4)))
    println(trie.existsSuperSet(List(1, 3)))
    println(trie.existsSuperSet(List(2, 5)))
    println(trie.existsSuperSet(List(2, 3, 4)))
  }
}
