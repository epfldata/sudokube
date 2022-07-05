package core.prepare

import core.materialization.MaterializationScheme
import planning.ProjectionMetaData
import util.Bits

//assumes query is of form 0..x and all cuboids are materialized
object FullLatticeOnlinePreparer extends Preparer {

  //only online query
  override def prepareOnline(m: MaterializationScheme, query: Seq[Int], cheap_size: Int, max_fetch_dim: Int): Seq[ProjectionMetaData] = {
      val PI = m.projections.zipWithIndex
      val qBS = query.toSet
      val qIS = query.toIndexedSeq
      val res = PI.flatMap {
        xid =>
          val pset = xid._1.toSet
          if (pset.size < cheap_size || !pset.diff(qBS).isEmpty) None else {
            val ab0 = pset // unnormalized
            val ab = qIS.indices.filter(i => ab0.contains(qIS(i))) // normalized
            val mask = Bits.mk_list_mask(xid._1, qBS)
            Some(ProjectionMetaData(ab, ab0, mask, xid._2))
          }
      }.sortBy(_.mask.length)
      res
  }
}
