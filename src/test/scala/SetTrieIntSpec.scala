import core.solver.SetTrie
import org.scalatest.{FlatSpec, Matchers}
import util.Util

import scala.util.Random

class SetTrieIntSpec extends FlatSpec with Matchers {

  "ExistsSuperSetInt " should " work " in {
    val trie = new SetTrie()
    trie.insertInt(List(1, 2, 3, 4, 5).map(1 << _).sum)
    trie.insertInt(List(10, 12, 15, 21, 25, 26).map(1 << _).sum)
    trie.insertInt(List(11, 12, 16, 18, 30, 31).map(1 << _).sum)
    trie.insertInt(List(1, 5, 10, 15, 20, 25, 30).map(1 << _).sum)
    trie.insertInt(List(1, 2, 3).map(1 << _).sum)
    trie.insertInt(List(4, 5, 6).map(1 << _).sum)
    trie.insertInt(List(1, 2, 3, 4).map(1 << _).sum)
    trie.insertInt(List(9, 11, 13).map(1 << _).sum)

    assert(trie.existsSuperSetInt(List(1, 2, 3, 4).map(1 << _).sum) == true)
    assert(trie.existsSuperSetInt(List(5, 10, 15).map(1 << _).sum) == true)
    assert(trie.existsSuperSetInt(List(5, 10, 20).map(1 << _).sum) == true)
    assert(trie.existsSuperSetInt(List(9, 11).map(1 << _).sum) == true)
    assert(trie.existsSuperSetInt(List(13, 25, 30).map(1 << _).sum) == false)
    assert(trie.existsSuperSetInt(List(1, 2).map(1 << _).sum) == true)
    assert(trie.existsSuperSetInt(0) == true)
    assert(trie.existsSuperSetInt(List(1, 2, 3, 4, 5).map(1 << _).sum) == true)
    assert(trie.existsSuperSetInt(List(13, 14, 15).map(1 << _).sum) == false)
    assert(trie.existsSuperSetInt(List(13, 15, 30).map(1 << _).sum) == false)
    assert(trie.existsSuperSetInt(List(9, 12, 25).map(1 << _).sum) == false)
    assert(trie.existsSuperSetInt(List(25, 26).map(1 << _).sum) == true)
  }

  def randomTest(ns: Int, ss: Int, max: Int, nq: Int, sq: Int)  = {
    val trie = new SetTrie()
    val sets = collection.mutable.ArrayBuffer[Set[Int]]()
    (0 until ns).foreach { i =>
      val s = Util.collect_n(ss,  () => Random.nextInt(max))
      trie.insertInt(s.sorted.map(1 << _).sum)
      sets += s.toSet
    }
    var trueCount = 0
    var falseCount = 0
    (0 until nq).foreach { i =>
      val q = Util.collect_n(sq, () => Random.nextInt(max))
      val s1 = trie.existsSuperSetInt(q.sorted.map(1 << _).sum)
      val s2 = sets.exists(p => q.toSet.subsetOf(p))
      if(s2) trueCount += 1
      else falseCount += 1
      assert(s1 == s2)
    }
    println(s"Trie Randomtest true = $trueCount false = $falseCount")
  }

  "Trie ExistSupersetInt" should s"be correct for random sets 1" in randomTest(10, 5, 20, 10, 2)
  "TrieInt ExistSupersetInt" should s"be correct for random sets 2" in randomTest(100, 5, 31, 100, 5)
  "TrieInt ExistSupersetInt" should s"be correct for random sets 3" in randomTest(1000, 10, 31, 1000, 5)
  "Trie ExistSupersetInt" should s"be correct for random sets 4" in randomTest(1000, 30, 31, 10000, 3)

}
