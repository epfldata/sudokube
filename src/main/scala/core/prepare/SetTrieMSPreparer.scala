package core.prepare

import core.SetTrieOnline
import core.materialization.{MaterializationScheme, SetTrieMaterializationScheme}
import planning.ProjectionMetaData
import util.Bits

object SetTrieMSPreparer extends Preparer {
  override def prepareOnline(m: MaterializationScheme, query: Seq[Int], cheap_size: Int, max_fetch_dim: Int): List[ProjectionMetaData] = {
    val qL = query.toList
    val qIS = query.toIndexedSeq
    val qBS = query.toSet
    val stMS = m.asInstanceOf[SetTrieMaterializationScheme]
    stMS.proj_trie.intersect(qL, List(), max_fetch_dim)
    val trie = new SetTrieOnline()
    var projs = List[ProjectionMetaData]()
    //decreasing order of projection size
    stMS.proj_trie.hm.toList.sortBy(x => -x._1.size).foreach { case (ab0, (c, id, p)) =>
      if (!trie.existsCheaperOrCheapSuperSet(ab0, c, cheap_size)) {
        val ab = qIS.indices.filter(i => ab0.contains(qIS(i))) // normalized
        val mask = Bits.mk_list_mask(p, qBS)
        projs = ProjectionMetaData(ab, ab0, mask, id) :: projs
        trie.insert(ab0, c)
      }
    }
    projs.sortBy(-_.accessible_bits.size)
  }
}
