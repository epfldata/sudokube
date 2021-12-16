//package ch.epfl.data.sudokube
package core
import planning._
import util._
import combinatorics._
import frontend.schema.Schema2

import scala.collection.BitSet

@SerialVersionUID(2L)
abstract class MaterializationScheme(val n_bits: Int) extends Serializable {

  /** the metadata describing each projection in this scheme. */
  val projections : IndexedSeq[List[Int]]


  object info {
    def wc_estimate(s: Int) = {
        projections.map(x => {
          val dense_size = Big.pow2(x.length)
          val sparse_full_db_size = Big.pow2(s)

          if(dense_size < sparse_full_db_size) dense_size
          else sparse_full_db_size
        }).sum
      }

    def fd_model_est(s: Int) : BigInt = {
      projections.map(x => FD_Model.avg_u(n_bits, s, x.length)).sum
    }

    def wc_ratio(s: Int) = BigDecimal(wc_estimate(s)) / BigDecimal(Big.pow2(s))

    def fd_ratio(s: Int) = {
      BigDecimal(fd_model_est(s)) / BigDecimal(Big.pow2(s))
    }

    def apply() {
      println("Upper bounds on sparse ratios for various # data items:"
        + " Kilo->" + wc_ratio(10)
        + " Mega->" + wc_ratio(20)
        + " Giga->" + wc_ratio(30)
        + " Tera->" + wc_ratio(40)
        + " Peta->" + wc_ratio(50)
        + " Exa->"  + wc_ratio(60))

      if(n_bits >= 60)
      println("In the FD model, the estimated storage overhead is:"
        + " Kilo->" + fd_model_est(10)
        + " Mega->" + fd_model_est(20)
        + " Giga->" + fd_model_est(30)
        + " Tera->" + fd_model_est(40)
        + " Peta->" + fd_model_est(50)
        + " Exa->"  + fd_model_est(60))
    }
  }


  /** create a plan for building each cuboid from the smallest that subsumes
      it. Using BestSubsumerPlanBuilder for this is too expensive though.

      The resulting build plan starts with the full cube and ends at the
      zero-dimensional cube. One can iterate through it from begin to end
      and dependencies will always be ready.

      The result is a list of triples (a, b, c), where
      a is the set of dimensions of the projection,
      b is the key by which the cuboid is to be indexed (which is also
      its index in projections), and
      c is the key of the cuboid from which it is to be built.
      The full cube will have -1 in field c.

      Note: the build plan construction is deterministic. Given the
      same projections (in the same order), it always produces the same
      result.
  */
  def create_build_plan() : List[(Set[Int], Int, Int)] = {
    // aren't they sorted by length by construction?
    val ps = projections.zipWithIndex.sortBy(_._1.length).reverse.toList
    assert(ps.head._1.length == n_bits)

    // the edges (_2, _3) form a tree rooted at the full cube
    var build_plan : List[(Set[Int], Int, Int)] =
      List((ps.head._1.toSet, ps.head._2, -1))

    val pi = new ProgressIndicator(ps.tail.length)

    ps.tail.foreach {
      case((l: List[Int]), (i: Int)) => {
        val s = l.toSet

        // first match is cheapest
        val y = build_plan.find{ case (s2, _, _) => s.subsetOf(s2) }
        y match {
          case Some((_, j, _)) => build_plan = (s, i, j) :: build_plan
          case None => assert(false)
        }
        pi.step
      }
    }
    println
    build_plan.reverse
  }

  def create_parallel_build_plan(nthreads: Int) = {
    val ps = projections.zipWithIndex.sortBy(_._1.length).reverse.toList
    assert(ps.head._1.length == n_bits)
    import collection.mutable.ArrayBuffer
    // the edges (_2, _3) form a tree rooted at the full cube
    var build_plan = collection.mutable.Map[Int,  ArrayBuffer[(BitSet, Int, Int)]]()
    build_plan ++=  (0 until nthreads).map{tid => tid -> ArrayBuffer((BitSet(ps.head._1:_*), ps.head._2, -1))}

    val thread_size = collection.mutable.Map[Int, Int]().withDefaultValue(0)
    val pi = new ProgressIndicator(ps.tail.length)

    ps.tail.foreach {
      case((l: List[Int]), (i: Int)) => {
        val s = BitSet(l :_*)
        // binary search for good parent. Not always the best parent
        val y = build_plan.mapValues { threadplan =>

          var idxB = threadplan.size - 1
          var idxA = 0
          var mid = (idxA + idxB)/2
          var cub = threadplan(mid)
          while(idxA <= idxB) {
            mid = (idxA + idxB)/2
            cub = threadplan(mid)
              if(s.subsetOf(cub._1)) {
                idxA = mid + 1
              } else {
                idxB = mid - 1
              }
          }
          var newiter = mid
          while(s.subsetOf(cub._1) && newiter < threadplan.size - 1) {
            newiter = newiter + 1
            cub = threadplan(newiter)
          }
          while(!s.subsetOf(cub._1)) {
            newiter = newiter - 1
            cub = threadplan(newiter)
          }
          //println(s"\nA=$idxA B=$idxB  mid=$mid  newmid=$newiter i=$i")
          cub
        }
        val y2 = y.tail.foldLeft(y.head){
          case (acc@(tida, (sa, _, _)), cur@(tidc, (sc, _, _))) =>
              if(sc.size < sa.size) cur
              else if((sc.size == sa.size) && thread_size(tidc) < thread_size(tida)) cur
              else acc
        }
        val (tid, (s2, j, pj)) = y2
        assert(s.subsetOf(s2))
        build_plan(tid) += ((s, i, j))
        thread_size(tid) += 1
        pi.step
      }
    }
    println
    build_plan.values.toList
  }


  /** projection without filtering.
      renormalizes: for each element of the result, .accessible_bits is
      a subset of the query, so we can now act as if the query was
      (0 to q.length - 1).
      We shift bits because otherwise the solver/optimizers can't handle them.
      This id is the index in projections. Assuming that the same ordering
      is used for the cuboids, it can be used to look up cuboids in the
      data cube.

      This returns as many elements as there are in projections.
      {{{
      assert(qproject(q).length == projections.length)
      }}}
  */
   def qproject(q: Seq[Int]): Seq[ProjectionMetaData] = {
    val PI = projections.zipWithIndex
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




  /** returns the metadata of cuboids that are suggested to be used to answer
      a given query. The results are ordered large cuboids (i.e. with many
      dimensions shared with the query) first.

      @param query          the query. The accessible bits of the resulting
                            ProjectionMetaData records are shifted as if the
                            query were (0 to query.length - 1). So the solver
                            does not need to know the actual query.
      @param cheap_size     cuboids below this size are only fetched if there
                            is no larger cuboid in our selection that subsumes
                            it.
                            //TODO: SBJ: It seems like dominee is removed only if dominator is cheap. It is not sufficient that dominee is cheap
      @param max_fetch_dim  the maximum dimensionality of cuboids to fetch.
                            This refers to their actual storage size, not the
                            number of dimensions shared with the query.

      Example:
      {{{
      scala> val dc = frontend.experiments.Tools.mkDC(5, 0.5, 2, 10)
      Creating materialization scheme...
      1/5/4/2/1/1
      scala> dc.m.projections
      res0: IndexedSeq[List[Int]] =
        Vector(List(), List(2), List(1), List(0), List(3), List(4),
               List(3, 4), List(0, 2), List(1, 3), List(0, 4),
               List(0, 1, 2), List(0, 1, 4), List(0, 1, 2, 4),
               List(0, 1, 2, 3, 4))
      scala> dc.m.prepare(List(0, 2, 4), 1, 3)
      prepare = List(List(0, 2), List(0, 1), List(1), List(0), List(2))
      res1: List[planning.ProjectionMetaData] =
        List(ProjectionMetaData(List(0, 2),List(0, 4),List(1, 1),9),
             ProjectionMetaData(List(0, 1),List(0, 2),List(1, 1),7),
             ProjectionMetaData(List(1),List(2),List(1),1),
             ProjectionMetaData(List(0),List(0),List(1),3),
             ProjectionMetaData(List(2),List(4),List(1),5))
      scala> dc.m.prepare(List(0,2,4), 3, 3)
      prepare = List(List(0, 2), List(0, 1))
      res2: List[planning.ProjectionMetaData] =
        List(ProjectionMetaData(List(0, 2),List(0, 4),List(1, 1),9),
             ProjectionMetaData(List(0, 1),List(0, 2),List(1, 1),7))
      scala> res2.foreach {
        pm => assert(dc.m.projections(pm.id) == pm.accessible_bits0) }
      }}}
  */
  def prepare(query: Seq[Int], cheap_size: Int, max_fetch_dim: Int
  ) : List[ProjectionMetaData] = {

    /* NOTE: finding the cheapest might not always preserve the one with the
       best sort order if we use storage with hierarchical sorting in bit order.
       Needs to be revisited should we ever have cuboids of varying sort
       orders.
    */
    val qp1: List[ProjectionMetaData] =
      qproject(query).filter(_.mask.length <= max_fetch_dim
      ).groupBy(_.accessible_bits).mapValues(l =>
        l.sortBy(_.mask.length).toList.head  // find cheapest: min mask.length
      ).toList.map(_._2)

    // remove those that are subsumed and the subsumer is cheap.
    val qp2 = qp1.filter(x => ! qp1.exists(y => y.dominates(x, cheap_size))
    ).sortBy(- _.accessible_bits.length) // high-dimensional ones first

    //println("prepare = " + qp2.map(_.accessible_bits))

/*
    println(qp2.length + " cuboids selected; cuboid sizes (bits->stored dimensions/cost->cuboid count): "
      + qp2.groupBy(_.accessible_bits.length).mapValues(x =>
      x.map(_.mask.length  // rather than _.cost_factor
      ).groupBy(y => y).mapValues(_.length)).toList.sortBy(_._1))
*/

    qp2
  }

  /** prepare for online aggregation. Evaluate in the order returned.
      The final cuboid will answer the query exactly by itself.
  */
  def prepare_online_agg(query: Seq[Int], cheap_size: Int
  ) : List[ProjectionMetaData] = {

    prepare(query, cheap_size, n_bits).sortBy(_.mask.length)
  }
}
object MaterializationScheme {
  def only_base_cuboid(n_bits: Int) = new MaterializationScheme(n_bits) {
    override val projections: IndexedSeq[List[Int]] = Vector((0 until n_bits).toList)
  }
}

/** TODO: also support the construction from a previously stored
    materialization scheme.
*/
case class PresetMaterializationScheme(
  _n_bits: Int,
  val projections : IndexedSeq[List[Int]]
) extends MaterializationScheme(_n_bits)


/**
    @param n       data dimensions, top cube: (0, ..., n - 1)
    @param rf      redundancy factor, multiplier for the number
                   of cubes for each size < n
    @param base    basis of power with which cuboid counts are computed.
                   a large base shifts the bulk of the storage costs down to
                   small-dimension cuboids. A large rf shifts it to
                   high-dimension cuboids.
    @param mindim  do not materialize cuboids of dimensionality less than
                   mindim (default 0)
*/

@SerialVersionUID(1L)
case class RandomizedMaterializationScheme(
  _n_bits: Int,
  rf: Double,
  base: Double,
  mindim: Int = 0
) extends MaterializationScheme(_n_bits) {

  /** build that many cuboids with d dimensions.
      {{{
      def plot(n: Int, rf: Double, base: Double, mindim: Int = 0) = {
        (0 to n).map(d => { print(n_proj_d(d) + "/") })
        println
      }
      }}}
  */
  def n_proj_d(d: Int) = {
    // no more than c many exist
    val c: BigInt = Combinatorics.comb(n_bits, d)
    val p = (rf * math.pow(base, n_bits - d)).toInt

    val np0 = if(d == n_bits) 1
    else if (d < mindim) 0
    else if(p < c) p
    else c.toInt

    // upper bound on number of cuboids of any given dimensionality
    if(np0 > 10000) 10000 else np0
  }

  println("Creating materialization scheme...")

  val projections : IndexedSeq[List[Int]] = {
    val r = (for(d <- 0 to n_bits - 1) yield {
        val n_proj = n_proj_d(d)

        print(n_proj + "/")

        Util.collect_n[List[Int]](n_proj, () =>
          Util.collect_n[Int](d, () =>
            scala.util.Random.nextInt(n_bits)).sorted)
      }
    ).flatten ++ Vector((0 to n_bits - 1).toList)
    println("1")
    r
  }
  println("Total = "+ projections.length)
} // end MaterializationScheme


@SerialVersionUID(1L)
case class SchemaBasedMaterializationScheme(sch: Schema2, logmaxND: Double, maxD: Int) extends MaterializationScheme(sch.n_bits) {
  /** the metadata describing each projection in this scheme. */
  override val projections: IndexedSeq[List[Int]] = {
    assert(sch.n_bits > maxD  * 2)
    val logmaxN = logmaxND.toInt
    val mod = logmaxND - logmaxN
    val maxPrefix = sch.root.numPrefixUpto(maxD)
    maxPrefix.zipWithIndex.foreach{case (n, i) => println(s"$i : $n")}

    val cubD = (0 until n_bits).map { d =>
      if (d <= maxD && d > maxD - logmaxN) {
        val logn = maxD-d
        val n = math.pow(2, logn + mod).toInt

       val res =  if (n < maxPrefix(d) * 0.9 - 10)
          Util.collect_n(n, () => sch.root.samplePrefix(d).toList.sorted).toVector
       else {
         (0 until (1.2 * n).toInt).map{i => sch.root.samplePrefix(d).toList}.distinct.toVector
       }
        print(res.length+"/")
        res
      } else {
        print("0/")
        Vector()
      }
    }
    println("1")
    val maxSize = maxD + math.log(logmaxND)/math.log(2) + mod
    println("Max total size = 2^"+ maxSize)
    cubD.reduce(_ ++ _) ++ Vector((0 until n_bits).toList)
  }
  println("Total ="+projections.length)

}

