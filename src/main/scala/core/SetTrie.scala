package core

import scala.collection.mutable.SortedSet

class SetTrie() {
  val root = Node(-1)

  def insert(s: List[Int], n: Node = root, ab0: List[Int] = List()): Unit = s match {
    //s is assumed to have distinct elements
    case Nil => ()
    case h :: t =>
      val c = n.findOrElseInsert(h)
      insert(t, c, ab0)
  }


  def existsSuperSet(s: List[Int], n: Node = root): Boolean = s match {
    case Nil => if (n.children.nonEmpty) true else false
    case h :: t =>
      var found = false
      val child = n.children.iterator
      var ce = n.b
      while (child.hasNext && !found && ce <= h) {
        val cn = child.next()
        ce = cn.b
        if (ce < h) {
          found = existsSuperSet(s, cn)
        } else if (ce == h) {
          found = existsSuperSet(t, cn)
        } else
          ()
      }
      found
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
        if (ce <= h && can_be_subset && cn.term_ab0.nonEmpty) {
          found = ((cn.term_ab0 :: found._1, found._2))
          cn.term_ab0 = List()
        }
        if (ce < h) {
          val ret = existsSuperSet_andGetSubsets(s, cn, false, can_be_superset)
          found = (ret._1 ::: found._1, ret._2 || found._2)
        } else if (ce == h) {
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

class SetTrieOnline() {
  val root = Node(-1)

  def insert(s: List[Int], this_cost: Int, n: Node = root): Unit = s match {
    //s is assumed to have distinct elements
    case Nil => ()
    case h :: t =>
      val c = n.findOrElseInsert(h, this_cost)
      insert(t, this_cost, c)
  }

  def existsCheaperOrCheapSuperSet(s: List[Int], s_cost: Int, cheap_size: Int, n: Node = root): Boolean = s match {
    case Nil => (n.cheapest_term <= s_cost || n.cheapest_term <= cheap_size)
    case h :: t =>
      var found = false
      val child = n.children.iterator
      var ce = n.b
      while (child.hasNext && !found && ce <= h && (n.cheapest_term <= s_cost || n.cheapest_term <= cheap_size)) {
        val cn = child.next()
        ce = cn.b
        if (ce < h) {
          found = existsCheaperOrCheapSuperSet(s, s_cost, cheap_size, cn)
        } else if (ce == h) {
          found = existsCheaperOrCheapSuperSet(t, s_cost, cheap_size, cn)
        } else
          ()
      }
      found
  }

  class Node(val b: Int, val children: SortedSet[Node], var cheapest_term: Int = 0) {
    def findOrElseInsert(v: Int, this_cost: Int): Node = {
      val c = children.find(_.b == v)
      c match {
        case None =>
          val nc = new Node(v, SortedSet(), this_cost)
          children += nc
          nc
        case Some(child) =>
          if (child.cheapest_term > this_cost)
            child.cheapest_term = this_cost
          child
      }
    }

  }

  object Node {
    implicit def order: Ordering[Node] = Ordering.by(_.b)

    def apply(i: Int) = new Node(i, SortedSet())
  }
}

class SetTrieIntersect() {
  val root = Node(-1)
  var hm = collection.mutable.HashMap[List[Int], (Int, Int, Seq[Int])]()
  var hm_accesses_0 = 0
  var hm_accesses_1 = 0

  /**
   * Inserts a projection into the SetTrieIntersect
   * Note : Nodes need to be inserted in increasing size to avoid overwriting Node as terminator.
   *
   * @param s      The projection
   * @param size   The size of the projection (s.length())
   * @param id     The id of the projection
   * @param full_p The projection again, to be saved later in the terminator.
   * @param n      The node to start inserting from
   */
  def insert(s: List[Int], size: Int, id: Int, full_p: List[Int], n: Node = root): Unit = s match {
    //s is assumed to have distinct elements
    case Nil => ()
    case h :: t =>
      if (t.isEmpty) {
        n.findOrElseInsert(h, size, true, id, full_p)
        ()
      } else {
        val c = n.findOrElseInsert(h, size, newId = id, newfull_p = full_p)
        insert(t, size, id, full_p, c)
      }
  }

  def intersect(s: List[Int], current_intersect: List[Int] = List(), max_fetch_dim: Int, n: Node = root): Unit = {
    if (n.isTerm && current_intersect.nonEmpty) {
      val res = hm.get(current_intersect)
      //println("A p " + n.full_p)
      if(res.isDefined){
        if(n.cheapest_term < res.get._1){
          hm(current_intersect) = (n.cheapest_term, n.id, n.full_p)
        }
      } else {
        hm(current_intersect) = (n.cheapest_term, n.id, n.full_p)
      }
    }
    s match {
      case Nil => val res = hm.get(current_intersect)
        //println("B p " + n.full_p)
        if(res.isDefined){
          if(hm(current_intersect)._2 == n.id){
          }
          if(n.cheapest_term < res.get._1){
            hm(current_intersect) = (n.cheapest_term, n.id, n.full_p)
          }
        } else {
          hm(current_intersect) = (n.cheapest_term, n.id, n.full_p)
        }
      case h :: t =>
        val child = n.children.iterator
        var ce = n.b
        while (child.hasNext && n.cheapest_term <= max_fetch_dim) {


          val cn = child.next()
          ce = cn.b
          var new_h = h
          var new_t = t


          while (ce > new_h && new_t.nonEmpty) {
            new_h = new_t.head
            new_t = new_t.tail
          }


          if (ce < new_h) {
            intersect(new_h :: new_t, current_intersect, max_fetch_dim, cn)
          } else if (ce == new_h) {
            intersect(new_t, current_intersect :+ new_h, max_fetch_dim, cn)
          } else {
            val res = hm.get(current_intersect)
            //println("C p " + cn.full_p)
            if(res.isDefined){
              if(hm(current_intersect)._2 == cn.id){
              }
              if(cn.cheapest_term < res.get._1){
                hm(current_intersect) = (cn.cheapest_term, cn.id, cn.full_p)
              }
            } else {
              hm(current_intersect) = (cn.cheapest_term, cn.id, cn.full_p)
            }
          }


        }
    }
  }

  def save_if_cheap(ab0: List[Int], cost: Int, id: Int, p: List[Int]): Unit = {
    val res = hm.get(ab0)
    if(res.isDefined){
      if(cost < res.get._1){
        hm(ab0) = (cost, id, p)
      }
    } else {
      hm(ab0) = (cost, id, p)
    }
  }

  def intersect2(s: List[Int], current_intersect: List[Int] = List(), max_fetch_dim: Int, n: Node = root): Unit = s match {
    case Nil => save_if_cheap(current_intersect, n.cheapest_term, n.id, n.full_p)
    case h :: t =>
      var found = false
      if(n.cheapest_term <= max_fetch_dim) {
        if (n.isTerm && current_intersect.nonEmpty) {
          //hm_accesses_0 += 1
          save_if_cheap(current_intersect, n.cheapest_term, n.id, n.full_p)
          found = true
        }
        val child = n.children.iterator
        var ce = n.b
        var continue = true
        while (child.hasNext && continue) {
          val cn = child.next()
          ce = cn.b
          var new_h = h
          var new_t = t
          while (ce > new_h && new_t.nonEmpty) {
            new_h = new_t.head
            new_t = new_t.tail
          }
          if (ce < new_h) {
            found = true
            intersect2(new_h :: new_t, current_intersect, max_fetch_dim, cn)
          } else if (ce == new_h) {
            found = true
            intersect2(new_t, current_intersect :+ new_h, max_fetch_dim, cn)
          } else
            continue = false
        }
        if(!found){
          //hm_accesses_1 += 1
          save_if_cheap(current_intersect, n.cheapest_term, n.id, n.full_p)
        }
      }
  }

  class Node(val b: Int, val children: SortedSet[Node], var cheapest_term: Int, val isTerm: Boolean = false, var id: Int = -1, var full_p: List[Int] = List()) {
    def findOrElseInsert(v: Int, this_cost: Int, newIsTerm: Boolean = false, newId: Int = -1, newfull_p: List[Int] = List()): Node = {
      val c = children.find(_.b == v)
      c match {
        case None =>
          val nc = new Node(v, SortedSet(), this_cost, newIsTerm, newId, newfull_p)
          children += nc
          nc
        case Some(child) =>
          if (child.cheapest_term > this_cost) {
            child.cheapest_term = this_cost
            child.id = this_cost
            child.full_p = newfull_p
          }
          if (newIsTerm)
            println("Error while inserting proj id : " + newId + "terminator node already exists.")
          child
      }
    }
  }

  object Node {
    implicit def order: Ordering[Node] = Ordering.by(_.b)

    def apply(i: Int) = new Node(i, SortedSet(), 0)
  }
}

class SetTrieBuildPlan() {
  val root = Node(-1)
  var cheapest_id = 0
  var cheapest_cost = Int.MaxValue

  def insert(s: List[Int], this_cost: Int, this_id: Int, n: Node = root): Unit = s match {
    //s is assumed to have distinct elements
    case Nil => ()
    case h :: t =>
      val c = n.findOrElseInsert(h, this_cost, this_id)
      insert(t, this_cost, this_id, c)
  }

  def extractCheapestSuperset(s: List[Int], n: Node = root): Unit = s match {
    case Nil =>
      if(n.cheapest_term < cheapest_cost){
        cheapest_id = n.ct_id
        cheapest_cost = n.cheapest_term
      }
    case h :: t =>
      val child = n.children.iterator
      var ce = n.b
      while (child.hasNext && ce <= h && n.cheapest_term < cheapest_cost) {
        val cn = child.next()
        ce = cn.b
        if (ce < h) {
          extractCheapestSuperset(s, cn)
        } else if (ce == h) {
          extractCheapestSuperset(t, cn)
        } else
          ()
      }
  }

  def getCheapestSuperset(s: List[Int]): Int = {
    extractCheapestSuperset(s)
    val ret = cheapest_id
    cheapest_id = 0
    cheapest_cost = Int.MaxValue
    ret
  }


  class Node(val b: Int, val children: SortedSet[Node], var cheapest_term: Int = 0, var ct_id: Int) {
    def findOrElseInsert(v: Int, this_cost: Int, this_id: Int): Node = {
      val c = children.find(_.b == v)
      c match {
        case None =>
          val nc = new Node(v, SortedSet(), this_cost, this_id)
          children += nc
          nc
        case Some(child) =>
          if (child.cheapest_term > this_cost) {
            child.cheapest_term = this_cost
            child.ct_id = this_id
          }
          child
      }
    }

  }

  object Node {
    implicit def order: Ordering[Node] = Ordering.by(_.b)

    def apply(i: Int) = new Node(i, SortedSet(), -100, -100)
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

object SetTrieOnline {
  def main(args: Array[String]): Unit = {
    val trie2 = new SetTrieOnline
    trie2.insert(List(1, 2, 4), 5)
    trie2.insert(List(1, 3, 5), 4)
    trie2.insert(List(1, 4), 8)
    trie2.insert(List(2, 3, 5), 3)
    trie2.insert(List(2, 4), 4)


    println(trie2.existsCheaperOrCheapSuperSet(List(2, 4), 4, 2))
  }
}

object SetTrieIntersect {
  def main(args: Array[String]): Unit = {
    val trieIntersect = new SetTrieIntersect
    trieIntersect.insert(List(1, 2, 5), 3, 1, List(1, 2, 5))
    trieIntersect.insert(List(4, 8, 20), 3, 2, List(4, 8, 20))
    trieIntersect.insert(List(2, 3, 4), 3, 4, List(2, 3, 4))
    trieIntersect.insert(List(4, 5, 10), 3, 5, List(4, 5))
    trieIntersect.insert(List(4, 10), 3, 5, List(4, 10))

    trieIntersect.intersect(List(4, 8), max_fetch_dim = 4)
    print(trieIntersect.hm)
  }
}
