package core

import collection.mutable.SortedSet

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
