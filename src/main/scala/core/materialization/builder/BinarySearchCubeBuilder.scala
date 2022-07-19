package core.materialization.builder

import core.materialization.MaterializationScheme
import util.ProgressIndicator

import scala.collection.immutable.BitSet
import scala.collection.mutable.ArrayBuffer

/**
 * Uses binary search within the available projection to find a good superset. Fast, but does not give the best
 * superset. Works well when most cuboids are to be projected from the base cuboid
 */
trait BinarySearchPlan {
  def create_build_plan(m: MaterializationScheme, showProgress: Boolean = false): Seq[(Set[Int], Int, Int)] = {
    val ps = m.projections.zipWithIndex.sortBy(_._1.length).reverse.toList
    assert(ps.head._1.length == m.n_bits)

    // the edges (_2, _3) form a tree rooted at the full cube
    val build_plan = ArrayBuffer[(BitSet, Int, Int)]()
    build_plan += ((BitSet(ps.head._1: _*), ps.head._2, -1))

    val pi = new ProgressIndicator(ps.tail.length, "Create BST build plan", showProgress)

    ps.tail.foreach {
      case ((l: IndexedSeq[Int]), (i: Int)) =>
        val s = BitSet(l: _*)
        // binary search for good parent. Not always the best parent
        var idxB = build_plan.size - 1
        var idxA = 0
        var mid = (idxA + idxB) / 2
        var cub = build_plan(mid)
        while (idxA <= idxB) {
          mid = (idxA + idxB) / 2
          cub = build_plan(mid)
          if (s.subsetOf(cub._1)) {
            idxA = mid + 1
          } else {
            idxB = mid - 1
          }
        }
        var newiter = mid
        //move right as much as possible
        while (s.subsetOf(cub._1) && newiter < build_plan.size - 1) {
          newiter = newiter + 1
          cub = build_plan(newiter)
        }
        //move left as much as possible
        while (!s.subsetOf(cub._1)) {
          newiter = newiter - 1
          cub = build_plan(newiter)
        }
        val (s2, j, pj) = cub
        assert(s.subsetOf(s2))
        build_plan += ((s, i, j))
        pi.step

    }
    if (showProgress) {
      println
    }
    build_plan
  }
}

object BinarySearchCubeBuilderST extends SingleThreadedCubeBuilder with BinarySearchPlan

object BinarySearchCubeBuilderMT extends MultiThreadedCubeBuilder with BinarySearchPlan