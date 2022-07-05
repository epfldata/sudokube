package core.prepare

import core.materialization.MaterializationScheme
import planning.ProjectionMetaData
import util.{Bits, Util}
import util.Util.intersect

//Refactored from prepare_opt by Eloi
object SetTrieOnlinePrepare5 extends Preparer {
  override def prepareOnline(m: MaterializationScheme, query: Seq[Int], cheap_size: Int, max_fetch_dim: Int): Seq[ProjectionMetaData] = {

    val qL = query.toList
    val qIS = query.toIndexedSeq
    val qBS = query.toSet
    import Util.intersect

    var abMapProjSingle = scala.collection.mutable.Map[IndexedSeq[Int], ProjectionMetaData]()
    var ret = List[ProjectionMetaData]()

    //For each projection, do filtering
    m.projections.zipWithIndex.foreach { case (p, id) =>
      if (p.size <= max_fetch_dim) {
        val ab0 = intersect(qL, p) //compute intersection
        val s = p.size
        val ab = qIS.indices.filter(i => ab0.contains(qIS(i)))
        val mask = Bits.mk_list_mask(p, qBS)
        //Only keep min mask.length when same ab
        if (abMapProjSingle.contains(ab)) {
          if (mask.length < abMapProjSingle(ab).mask.length) {
            abMapProjSingle -= ab
            val newp = (ab -> ProjectionMetaData(ab, ab0, mask, id))
            if (!abMapProjSingle.exists(y => y._2.dominates(newp._2, cheap_size))) {
              abMapProjSingle = abMapProjSingle.filter(x => !newp._2.dominates(x._2))
              abMapProjSingle += newp
            }
            //abMapProjSingle += (ab -> ProjectionMetaData(ab, ab0, mask, id))
          }
        } else {
          val newp = (ab -> ProjectionMetaData(ab, ab0, mask, id))
          if (!abMapProjSingle.exists(y => y._2.dominates(newp._2, cheap_size))) {
            abMapProjSingle = abMapProjSingle.filter(x => !newp._2.dominates(x._2))
            abMapProjSingle += newp
          }
        }
      }
    }

    //Final filtering for dominating cuboids, could be optimized

    abMapProjSingle.values.toList.sortBy(-_.accessible_bits.length)
    /*abMapProjSingle.filter(x => !abMapProjSingle.exists(y => y._2.dominates(x._2, cheap_size))).values.toList.sortBy(-_.accessible_bits.length)
    */

    /* Simpler implem, no performance difference
    ret = abMapProjSingle.values.toList
    ret.filter(x => !ret.exists(y => y.dominates(x, cheap_size))
    ).sortBy(-_.accessible_bits.length)

     */

  }
}
