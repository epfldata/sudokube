package core.prepare

import core.materialization.MaterializationScheme
import core.solver.SetTrieOnline
import planning.ProjectionMetaData
import util.{Bits, Util}
import util.Util.intersect

//Refactored from prepare_online_new by Eloi
object SetTrieOnlinePrepareNoInt extends Preparer {
  /** Optimized version of prepare for online mode. Uses the adapted SetTrie to check for cheap superset
   * returns the metadata of cuboids that are suggested to be used to answer
   * a given query. The results are ordered large cuboids (i.e. with many
   * dimensions shared with the query) first.
   * @param query         the query. The accessible bits of the resulting
   *                      ProjectionMetaData records are shifted as if the
   *                      query were (0 to query.length - 1). So the solver
   *                      does not need to know the actual query.
   * @param cheap_size    cuboids below this size are only fetched if there
   *                      is no larger cuboid in our selection that subsumes
   *                      it.
   * @param max_fetch_dim the maximum dimensionality of cuboids to fetch.
   *                      This refers to their actual storage size, not the
   *                      number of dimensions shared with the query.
   */
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
