package core.prepare

import core.materialization.MaterializationScheme
import core.solver.SetTrie
import planning.ProjectionMetaData
import util.{Bits, Profiler, Util}
import util.Util.{intersect, intersect_intval3}

//Refactored from prepare_batch_new by Eloi
object SetTrieBatchPrepareWithInt extends Preparer {
  /** Optimized version of prepare for batch mode.
   * Equivalent to prepare_new, but with integer representation of intersection for faster hashmap accesses.
   * @param query         the query. The accessible bits of the resulting
   *                      ProjectionMetaData records are shifted as if the
   *                      query were (0 to query.length - 1). So the solver
   *                      does not need to know the actual query.
   * @param cheap_size    unused, expected to be >= max_fetch_dim in batch mode
   * @param max_fetch_dim the maximum dimensionality of cuboids to fetch.
   *                      This refers to their actual storage size, not the
   *                      number of dimensions shared with the query.
   */
  override def prepareBatch(m: MaterializationScheme, query: Seq[Int], max_fetch_dim: Int): Seq[ProjectionMetaData] = {
    val qL = query.toList
    val qIS = query.toIndexedSeq
    val qBS = query.toSet
    val hm = collection.mutable.HashMap[Int, (Int, Int, Seq[Int], List[Int])]()

    m.projections.zipWithIndex.foreach { case (p, id) =>
      if (p.size <= max_fetch_dim) {
        val (ab0, ab0_i) = intersect_intval3(qL, p)
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

    val trie = new SetTrie()
    var projs = List[ProjectionMetaData]()
    //decreasing order of projection size
    hm.toList.sortBy(x => -x._2._4.size).foreach { case (ab0_i, (c, id, p, ab0)) =>
      if (!trie.existsSuperSet(ab0)) {
        val ab = qIS.indices.filter(i => ab0.contains(qIS(i))) // normalized
        val mask = Bits.mk_list_mask(p, qBS)
        projs = ProjectionMetaData(ab, ab0, mask, id) :: projs
        trie.insert(ab0)
      }
    }
    projs.sortBy(-_.accessible_bits.size)
  }
}