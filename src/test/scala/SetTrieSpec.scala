import core.SetTrie
import org.scalatest.{FlatSpec, Matchers}
import util.Util

import scala.util.Random

class SetTrieSpec extends FlatSpec with Matchers {

  "ExistsSuperSet " should " work " in {
    val trie = new SetTrie()
    trie.insert(List(1, 2, 3, 4, 5))
    trie.insert(List(10, 12, 15, 21, 25, 26))
    trie.insert(List(11, 12, 16, 18, 30, 31))
    trie.insert(List(1, 5, 10, 15, 20, 25, 30))
    trie.insert(List(1, 2, 3))
    trie.insert(List(4, 5, 6))
    trie.insert(List(1, 2, 3, 4))
    trie.insert(List(9, 11, 13))

    assert(trie.existsSuperSet(List(1, 2, 3, 4)) == true)
    assert(trie.existsSuperSet(List(5, 10, 15)) == true)
    assert(trie.existsSuperSet(List(5, 10, 20)) == true)
    assert(trie.existsSuperSet(List(9, 11)) == true)
    assert(trie.existsSuperSet(List(13, 25, 30)) == false)
    assert(trie.existsSuperSet(List(1, 2)) == true)
    assert(trie.existsSuperSet(List()) == true)
    assert(trie.existsSuperSet(List(1, 2, 3, 4, 5)) == true)
    assert(trie.existsSuperSet(List(13, 14, 15)) == false)
    assert(trie.existsSuperSet(List(13, 15, 30)) == false)
    assert(trie.existsSuperSet(List(9, 12, 25)) == false)
    assert(trie.existsSuperSet(List(25, 26)) == true)
  }

  def randomTest(ns: Int, ss: Int, max: Int, nq: Int, sq: Int)  = {
    val trie = new SetTrie()
    val sets = collection.mutable.ArrayBuffer[Set[Int]]()
    (0 until ns).foreach { i =>
      val s = Util.collect_n(ss,  () => Random.nextInt(max))
      trie.insert(s.sorted)
      sets += s.toSet
    }
    var trueCount = 0
    var falseCount = 0
    (0 until nq).foreach { i =>
      val q = Util.collect_n(sq, () => Random.nextInt(max))
      val s1 = trie.existsSuperSet(q.sorted)
      val s2 = sets.exists(p => q.toSet.subsetOf(p))
      if(s2) trueCount += 1
      else falseCount += 1
      assert(s1 == s2)
    }
    print(s"Trie Randomtest true = $trueCount false = $falseCount")
  }

  "Trie ExistSuperset" should s"be correct for random sets 1" in randomTest(10, 5, 20, 10, 2)
  "Trie ExistSuperset" should s"be correct for random sets 2" in randomTest(100, 5, 1000, 100, 5)
  "Trie ExistSuperset" should s"be correct for random sets 3" in randomTest(1000, 10, 1000, 1000, 5)
  "Trie ExistSuperset" should s"be correct for random sets 4" in randomTest(1000, 100, 1000, 10000, 3)
}
