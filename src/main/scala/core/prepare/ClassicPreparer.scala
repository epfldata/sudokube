package core.prepare

import core.materialization.MaterializationScheme
import planning.ProjectionMetaData
import util.Bits

object ClassicPreparer extends Preparer {

  /** Projection without filtering.
   * Renormalizes: for each element of the result, .accessible_bits is
   * a subset of the query, so we can now act as if the query was
   * (0 to q.length - 1).
   * We shift bits because otherwise the solver/optimizers can't handle them.
   * This id is the index in projections. Assuming that the same ordering
   * is used for the cuboids, it can be used to look up cuboids in the
   * data cube.
   *
   * This returns as many elements as there are in projections.
   * {{{
   *    assert(qproject(q).length == projections.length)
   * }}}
   */

  def qproject(m: MaterializationScheme, q: Seq[Int]): Seq[ProjectionMetaData] = {
    val PI = m.projections.zipWithIndex
    val qBS = q.toSet
    val qIS = q.toIndexedSeq
    val res = PI.map { xid =>
      val ab0 = xid._1.toSet.intersect(qBS) // unnormalized
      val ab = qIS.indices.filter(i => ab0.contains(qIS(i))) // normalized
      val mask = Bits.mk_list_mask(xid._1, qBS)
      ProjectionMetaData(ab, ab0, mask, xid._2)
    }
    res
  }

  override def prepareBatch(m: MaterializationScheme, query: Seq[Int], max_fetch_dim: Int): Seq[ProjectionMetaData] = prepareClassic(m, query, max_fetch_dim, max_fetch_dim)

  /** prepare for online aggregation. Evaluate in the order returned.
   * The final cuboid will answer the query exactly by itself.
   */
  override def prepareOnline(m: MaterializationScheme, query: Seq[Int], cheap_size: Int, max_fetch_dim: Int): Seq[ProjectionMetaData] = prepareClassic(m, query, max_fetch_dim, max_fetch_dim).sortBy(_.mask.length)

  def prepareClassic(m: MaterializationScheme, query: Seq[Int], cheap_size: Int, max_fetch_dim: Int): Seq[ProjectionMetaData] = {

    /* NOTE: finding the cheapest might not always preserve the one with the
       best sort order if we use storage with hierarchical sorting in bit order.
       Needs to be revisited should we ever have cuboids of varying sort
       orders.
    */

    val qp0 = qproject(m, query).filter(_.mask.length <= max_fetch_dim)

    val qp1: List[ProjectionMetaData] =

      qp0.groupBy(_.accessible_bits).mapValues(l =>
        l.sortBy(_.mask.length).toList.head // find cheapest: min mask.length
      ).toList.map(_._2)

    // remove those that are subsumed and the subsumer is cheap.
    val qp2 = qp1.filter(x => !qp1.exists(y => y.dominates(x, cheap_size))
    ).sortBy(-_.accessible_bits.length) // high-dimensional ones first

    //println("prepare = " + qp2.map(_.accessible_bits))

    /*
    println(qp2.length + " cuboids selected; cuboid sizes (bits->stored dimensions/cost->cuboid count): "
      + qp2.groupBy(_.accessible_bits.length).mapValues(x =>
      x.map(_.mask.length  // rather than _.cost_factor
      ).groupBy(y => y).mapValues(_.length)).toList.sortBy(_._1))
*/

    qp2
  }
}