package core.prepare

import core.SetTrieOnline
import core.materialization.MaterializationScheme
import planning.ProjectionMetaData
import util.{Bits, Util}
import util.Util.intersect

//Refactored from prepare_online_new by Eloi
object SetTrieOnlinePrepare1 extends Preparer {
  override def prepareOnline(m: MaterializationScheme, query: Seq[Int], cheap_size: Int, max_fetch_dim: Int): Seq[ProjectionMetaData] = {
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

    val trie = new SetTrieOnline()
    var projs = List[ProjectionMetaData]()
    //decreasing order of projection size
    hm.toList.sortBy(x => -x._1.size).foreach { case (ab0, (c, id, p)) =>
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
