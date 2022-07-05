package core.prepare

import core.materialization.MaterializationScheme
import planning.ProjectionMetaData
import util.Bits


trait Preparer {

  /** Returns the metadata of cuboids that are suggested to be used to answer
   * a given query. The results are ordered large cuboids (i.e. with many
   * dimensions shared with the query) first.
   *
   * @param query         the query. The accessible bits of the resulting
   *                      ProjectionMetaData records are shifted as if the
   *                      query were (0 to query.length - 1). So the solver
   *                      does not need to know the actual query.
   * @param cheap_size    cuboids below this size are only fetched if there
   *                      is no larger cuboid in our selection that subsumes
   *                      it.
   *                      //TODO: SBJ: It seems like dominee is removed only if dominator is cheap. It is not sufficient that dominee is cheap
   * @param max_fetch_dim the maximum dimensionality of cuboids to fetch.
   *                      This refers to their actual storage size, not the
   *                      number of dimensions shared with the query.
   * @example {{{
   *  scala> val dc = frontend.experiments.Tools.mkDC(5, 0.5, 2, 10)
   *    Creating materialization scheme...
   *    1/5/4/2/1/1
   *  scala> dc.m.projections
   *    res0: IndexedSeq[List[Int]] =
   *    Vector(List(), List(2), List(1), List(0), List(3), List(4),
   *    List(3, 4), List(0, 2), List(1, 3), List(0, 4),
   *    List(0, 1, 2), List(0, 1, 4), List(0, 1, 2, 4),
   *    List(0, 1, 2, 3, 4))
   *  scala> dc.m.prepare(List(0, 2, 4), 1, 3)
   *    prepare = List(List(0, 2), List(0, 1), List(1), List(0), List(2))
   *    res1: List[planning.ProjectionMetaData] =
   *    List(ProjectionMetaData(List(0, 2),List(0, 4),List(1, 1),9),
   *    ProjectionMetaData(List(0, 1),List(0, 2),List(1, 1),7),
   *    ProjectionMetaData(List(1),List(2),List(1),1),
   *    ProjectionMetaData(List(0),List(0),List(1),3),
   *    ProjectionMetaData(List(2),List(4),List(1),5))
   *  scala> dc.m.prepare(List(0,2,4), 3, 3)
   *    prepare = List(List(0, 2), List(0, 1))
   *    res2: List[planning.ProjectionMetaData] =
   *    List(ProjectionMetaData(List(0, 2),List(0, 4),List(1, 1),9),
   *    ProjectionMetaData(List(0, 1),List(0, 2),List(1, 1),7))
   *  scala> res2.foreach {
   *    pm => assert(dc.m.projections(pm.id) == pm.accessible_bits0) }
   * }}}
   */


  def prepareBatch(m: MaterializationScheme, query: Seq[Int], max_fetch_dim: Int): Seq[ProjectionMetaData] = prepareOnline(m, query, max_fetch_dim, max_fetch_dim)
  def prepareOnline(m: MaterializationScheme, query: Seq[Int], cheap_size: Int, max_fetch_dim: Int): Seq[ProjectionMetaData] = ???
}

object Preparer {
  val default: Preparer = new Preparer {
    override def prepareBatch(m: MaterializationScheme, query: Seq[Int], max_fetch_dim: Int): Seq[ProjectionMetaData] = SetTrieBatchPrepare1.prepareBatch(m, query, max_fetch_dim)
    override def prepareOnline(m: MaterializationScheme, query: Seq[Int], cheap_size: Int, max_fetch_dim: Int): Seq[ProjectionMetaData] = ClassicPreparer.prepareOnline(m, query, cheap_size, max_fetch_dim)
  }
}


