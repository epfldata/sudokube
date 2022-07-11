package core.prepare

import core.SetTrie
import core.materialization.MaterializationScheme
import planning.ProjectionMetaData
import util.{Bits, Profiler, Util}

//Refactored prepare_new() method by SBJ
object SetTrieBatchPrepare1 extends Preparer {
  override def prepareBatch(m: MaterializationScheme, query: Seq[Int], max_fetch_dim: Int): Seq[ProjectionMetaData] = {
    val qL = query.toList
    val qIS = query.toIndexedSeq
    val qBS = query.toSet
    val hm = collection.mutable.HashMap[List[Int], (Int, Int, Seq[Int])]()
    import Util.intersect

    m.projections.zipWithIndex.foreach { case (p, id) =>
      if (p.size <= max_fetch_dim) {
        val ab0 = intersect(qL, p)
        val res = hm.get(ab0)
        val s = p.size

        if (res.isDefined) {
          if (s < res.get._1)
            hm(ab0) = (s, id, p)
        } else {
          hm(ab0) = (s, id, p)
        }
      }
    }

    val trie = new SetTrie()
    var projs = List[ProjectionMetaData]()
    //decreasing order of projection size
    hm.toList.sortBy(x => -x._1.size).foreach { case (s, (c, id, p)) =>
      if (!trie.existsSuperSet(s)) {
        val ab = qIS.indices.filter(i => s.contains(qIS(i))) // normalized
        val mask = Bits.mk_list_mask(p, qBS)
        projs = ProjectionMetaData(ab, s, mask, id) :: projs
        trie.insert(s)
      }
    }
    projs.sortBy(-_.accessible_bits.size)
  }
}
