package core

import scala.collection.mutable.SortedSet

import util.Bits

//we assume every node has moment stored
@SerialVersionUID(1L)
class SetTrieForMoments(maxSize: Int = 1024) extends Serializable {
  val nodes = Array.fill(maxSize)(new Node(-1, Double.NaN, -1, -1))
  var count = 0

  //insert all moments of cuboid
  def insertAll(cuboid: List[Int], moments: Array[Double]) = {
    val cArray = cuboid.sorted.toArray
    moments.indices.foreach { i =>
      val ls = Bits.fromInt(i).map(x => cArray(x)).sorted
      //println(s"Insert $i@$ls = ${moments(i)} ")
      if(count < maxSize) insert(ls, moments(i), nodes(0))
    }
  }

  def getNormalizedSubsetMoments(q: List[Int]): List[(Int, Double)] = {
    val qarray = q.toArray

    def rec(n: Node, qidx: Int, branch: Int): List[(Int, Double)] = {
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

  def insert(s: List[Int], moment: Double, n: Node = nodes(0)): Unit = s match {
    //s is assumed to have distinct elements
    //all subsets assumed to be inserted before some set
    case Nil if count == 0 =>
      nodes(0).moment = moment
      count = 1
    case Nil => ()
    case h :: t =>
      if (t.isEmpty || n.moment != 0.0) { //do not insert child 0 moments
        val c = findOrElseInsert(n, h, moment)
        if (t.isEmpty) assert(c.moment == moment) else
          insert(t, moment, c)
      }
  }

  def findOrElseInsert(n: Node, v: Int, moment: Double) = {
    var cur = n.firstChild
    var child = if(cur != -1) nodes(cur) else null
    var prev: Node = null

    while (cur != -1 && child.b < v) {
      prev = child
      cur = child.nextSibling
      child = if(cur != -1) nodes(cur) else null
    }
    if (child != null && child.b == v) {
      child
    } else {
      val newnode = nodes(count)
      newnode.set(v, moment, cur)
      if (prev == null)
        n.firstChild = count
      else
        prev.nextSibling = count
      count += 1
      newnode
    }
  }
}

@SerialVersionUID(2L)
class Node(var b: Int, var moment: Double, var firstChild: Int, var nextSibling: Int) extends Serializable {
  def set(bval: Int, mval: Double, sibVal: Int) = {
    b = bval
    moment = mval
    nextSibling = sibVal
  }

}

class SetTrie() {
  val root = Node(-1)

  def insert(s: List[Int], n: Node = root): Unit = s match {
    //s is assumed to have distinct elements
    case Nil => ()
    case h :: t =>
      val c = n.findOrElseInsert(h)
      insert(t, c)
  }


  def existsSuperSet(s: List[Int], n: Node = root): Boolean = s match {
    case Nil => true
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

/**
 * A SetTrie representation of a Set of Lists of Integers.
 * Used for prepare online mode, to check for domination.
 */
class SetTrieOnline() {
  val root = Node(-1)

  def insert(s: List[Int], this_cost: Int, n: Node = root): Unit = s match {
    //s is assumed to have distinct elements
    case Nil => ()
    case h :: t =>
      val c = n.findOrElseInsert(h, this_cost)
      insert(t, this_cost, c)
  }

  /** Returns true if there exists a Set in the SetTrie that is a superset of the given List and is cheaper
   *
   * @param s The list for which we want to check existence of cheap/cheaper superset
   * @param s_cost The cost of the list, usually it's length
   * @param cheap_size The size under which any List is considered cheap
   * @param n The node at which the search starts
   * @return Whether there exists a cheap/cheaper superset
   */
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

/**
 * A SetTrie representation of a Set of Lists of Integers.
 * Unsuccessful experiment, but interesting nonetheless
 * Allows to compute all intersections in a single DFS exploration
 */
class SetTrieIntersect() {
  val root = Node(-1)
  var hm = collection.mutable.HashMap[List[Int], (Int, Int, Seq[Int])]()
  var hm_accesses = 0

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

  /**
   * Computes the intersection of the given List with every List stored in the SetTrieIntersect
   * @param s The List for which we will compute the interesection
   * @param current_intersect The current intersection at the current node
   * @param max_fetch_dim Any projection over this size will not be considered
   * @param n The node at which to start
   */
  def intersect(s: List[Int], current_intersect: List[Int] = List(), max_fetch_dim: Int, n: Node = root): Unit = {
    if (n.isTerm && current_intersect.nonEmpty) {
      save_if_cheap(current_intersect, n.cheapest_term, n.id, n.full_p)
    }
    s match {
      case Nil => save_if_cheap(current_intersect, n.cheapest_term, n.id, n.full_p)
      case h :: t =>
        val child = n.children.iterator
        var ce = n.b
        var continue = true
        while (child.hasNext && n.cheapest_term <= max_fetch_dim && continue) {
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
            if(new_t.isEmpty){
              continue = false
            }
            intersect(new_t, current_intersect :+ new_h, max_fetch_dim, cn)
          } else {
            save_if_cheap(current_intersect, cn.cheapest_term, cn.id, cn.full_p)
          }
        }
    }
  }

  /**
   * Saves the intersection in the hashmap if it is cheaper than a previous projection with the same intersection
   * @param ab0 The intersection to save
   * @param cost The cost of said intersection
   * @param id The id of the projection
   * @param p The projection from which the intersection comes
   */
  def save_if_cheap(ab0: List[Int], cost: Int, id: Int, p: List[Int]): Unit = {
    hm_accesses += 1
    val res = hm.get(ab0)
    if(res.isDefined){
      if(cost < res.get._1){
        hm(ab0) = (cost, id, p)
      }
    } else {
      hm(ab0) = (cost, id, p)
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
    trie.insert(List(10, 12, 15, 21, 25, 26))

    println(trie.existsSuperSet(List(3, 5)))
    println(trie.existsSuperSet(List(2, 6)))
    println(trie.existsSuperSet(List(4)))
    println(trie.existsSuperSet(List(1, 3)))
    println(trie.existsSuperSet(List(2, 5)))
    println(trie.existsSuperSet(List(2, 3, 4)))
    println(trie.existsSuperSet(List(25, 26)))
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
