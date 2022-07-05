package core.prepare
import core.SetTrieOnline
import core.materialization.MaterializationScheme
import planning.ProjectionMetaData
import util.{Bits, Util}

//Refactored from prepare_online_new_int by Eloi
object SetTrieOnlinePrepare2 extends Preparer {
  override def prepareOnline(m: MaterializationScheme, query: Seq[Int], cheap_size: Int, max_fetch_dim: Int): Seq[ProjectionMetaData] =  {
    val qL = query.toList
    val qIS = query.toIndexedSeq
    val qBS = query.toSet
    val hm = collection.mutable.HashMap[Int, (Int, Int, Seq[Int], List[Int])]()
    import Util.intersect_intval

    m.projections.zipWithIndex.foreach { case (p, id) =>
      if (p.size <= max_fetch_dim) {
        val (ab0, ab0_i) = intersect_intval(qL, p)
        val res = hm.get(ab0_i)
        val s = p.size

        if (res.isDefined) {
          if (s < res.get._1)
            hm(ab0_i) = (s, id, p, ab0)
        } else {
          hm(ab0_i) = (s, id, p, ab0)
        }
      }
    }

    val trie = new SetTrieOnline()
    var projs = List[ProjectionMetaData]()
    //decreasing order of projection size
    hm.toList.sortBy(x => -x._2._4.size).foreach { case (ab0_i, (c, id, p, ab0)) =>
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
