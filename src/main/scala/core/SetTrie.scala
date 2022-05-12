package core

import collection.mutable.SortedSet

class SetTrie() {
  val root =  Node(-1)

  def insert(s: List[Int], n: Node = root, ab0: List[Int] = List()): Unit = s match {
    //s is assumed to have distinct elements
    case Nil => ()
    case h :: Nil =>
      val c = n.findOrElseInsert(h, ab0)
      insert(Nil, c, ab0)
    case h :: t =>
      val c = n.findOrElseInsert(h)
      insert(t, c, ab0)
  }


  def existsSuperSet(s: List[Int], n: Node = root): Boolean = s match {
    case Nil => if(n.children.nonEmpty) true else false
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

  def existsCheaperOrCheapSuperSet(s: List[Int], s_cost: Int, cheap_size: Int, curr_size: Int = 1, n: Node = root): Boolean = s match {
    case Nil => if(n.children.nonEmpty) true else false
    case h :: t =>
      if(curr_size > cheap_size && curr_size > s_cost){
        false
      } else {
        var found = false
        val child = n.children.iterator
        var ce = n.b
        while (child.hasNext && !found && ce <= h) {
          val cn = child.next()
          ce = cn.b
          if(ce < h) {
            found = existsCheaperOrCheapSuperSet(s, s_cost, cheap_size, curr_size+1, cn)
          } else if(ce == h) {
            found = existsCheaperOrCheapSuperSet(t, s_cost, cheap_size, curr_size+1, cn)
          } else
            ()
        }
        found
      }


  }

  def existsSuperSet_andGetSubsets(s: List[Int], n: Node = root, can_be_subset: Boolean, can_be_superset: Boolean): (List[List[Int]], Boolean) = s match {
    case Nil => (List(), true)
    case h :: t =>
      var found = (List[List[Int]](), false)
      val child = n.children.iterator
      var ce = n.b
      while (child.hasNext && ce <= h) {
        val cn = child.next()
        ce = cn.b
        if(ce <= h && can_be_subset && cn.term_ab0.nonEmpty){
          found = ((cn.term_ab0 :: found._1, found._2))
          cn.term_ab0 = List()
        }
        if(ce < h) {
          val ret = existsSuperSet_andGetSubsets(s, cn, false, can_be_superset)
          found = (ret._1 ::: found._1, ret._2 || found._2)
        } else if(ce == h) {
          val ret = existsSuperSet_andGetSubsets(t, cn, can_be_subset, can_be_subset)
          found = (ret._1 ::: found._1, ret._2 || found._2)
        } else
          ()
      }
      found
  }

  class Node(val b: Int, val children: SortedSet[Node], var term_ab0: List[Int] = List()) {
    def findOrElseInsert(v: Int, ab0: List[Int] = List()) = {
      val c = children.find(_.b == v)
      if (c.isDefined)
        c.get
      else {
        val nc = new Node(v, SortedSet(), ab0)
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
