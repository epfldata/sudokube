import core.solver.Moment1Transformer
import core.{SetTrie, SetTrieForMoments}
import org.scalatest.{FlatSpec, Matchers}
import util.{BigBinary, Bits, Util}

import java.io.{File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import scala.math.Numeric.Implicits.infixNumericOps
import scala.reflect.ClassTag
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
    println(s"Trie Randomtest true = $trueCount false = $falseCount")
  }

  "Trie ExistSuperset" should s"be correct for random sets 1" in randomTest(10, 5, 20, 10, 2)
  "Trie ExistSuperset" should s"be correct for random sets 2" in randomTest(100, 5, 1000, 100, 5)
  "Trie ExistSuperset" should s"be correct for random sets 3" in randomTest(1000, 10, 1000, 1000, 5)
  "Trie ExistSuperset" should s"be correct for random sets 4" in randomTest(1000, 100, 1000, 10000, 3)

  "SetTrie" should "return subset moments for query " in {
    val trie = new SetTrieForMoments()
    val nbits = 10
    val N = (1 << nbits)
    (0 until N).foreach { i =>
      val cs = Bits.fromInt(i).sorted
      trie.insert(cs, (i+1).toDouble)
    }
    assert(trie.count == N)
    def query(q: List[Int]) = {
      val moments = trie.getNormalizedSubsetMoments(q).toMap
      val N = (1 << q.length)
      assert(moments.size == N)
      (0 until N).foreach { i =>
        //println(s"i = $i  m = ${moments(i)}")
        assert(moments(i).toInt-1 == BigBinary(i).pup(q).toInt)
      }
    }

    query(List())
    query((0 until nbits).toList)
    query(List(5, 7, 9))
    query(List(2, 5, 6))
    query(List(0, 5, 6, 7))
    query(List(0, 2, 3, 4, 8))
    query(List(1, 3, 4))
  }

  "SetTrie" should "return subset moments from cuboids for query " in {
    val trie = new SetTrieForMoments()
    def ms[T:ClassTag:Fractional](vs : Array[T]) = Moment1Transformer[T]().getMoments(vs)
    trie.insertAll(List(0, 1), ms(Array(7, 3, 6, 1)))
    trie.insertAll(List(1, 2), ms(Array(1, 4, 9, 3)))
    trie.insertAll(List(0, 2), ms(Array(3, 2, 10, 2)))
    assert(trie.count == 7)
    val filename = "triemoment"
    val file = new File("cubedata/" + filename + "/" + filename + ".trie")
    if(!file.exists())
      file.getParentFile.mkdirs()
    val oos = new ObjectOutputStream(new FileOutputStream(file))
    oos.writeObject(trie)


    val ois = new ObjectInputStream(new FileInputStream(file))
    val trie2 = ois.readObject().asInstanceOf[SetTrieForMoments]
    assert(trie2.count == 7)
    val q = List(0, 1, 2)
    val moments = trie2.getNormalizedSubsetMoments(q).sortBy(_._1)
    val actual = List(17, 4, 7, 1, 12, 2, 3).zipWithIndex.map{case (m, i) => (i, m.toDouble)}
    assert(moments.sameElements(actual))
    file.delete
    file.getParentFile.delete()
  }
}
