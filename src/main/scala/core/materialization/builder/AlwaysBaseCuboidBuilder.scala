package core.materialization.builder

import core.materialization.MaterializationStrategy
import util.ProgressIndicator

/**
 * Searches among all currently materialized cuboids to determine what to project
 * Always uses the base cuboid.
 */
trait AlwaysBaseCuboidBuilder {


  def create_build_plan(m: MaterializationStrategy, showProgress: Boolean = false): List[(Set[Int], Int, Int)] = {
    // aren't they sorted by length by construction?
    val ps = m.projections.zipWithIndex.sortBy(_._1.length).reverse.toList
    assert(ps.head._1.length == m.n_bits)


    val baseCuboidSet = ps.head._1.toSet
    val baseCuboidId = ps.head._2

    // the edges (_2, _3) form a tree rooted at the full cube
    var build_plan: List[(Set[Int], Int, Int)] =
      List((baseCuboidSet, baseCuboidId, -1))


    val pi = new ProgressIndicator(ps.tail.length, "Always Base Cuboid Build Plan", showProgress)

    ps.tail.foreach {
      case (l: IndexedSeq[Int], i: Int) =>
        val s = l.toSet
        build_plan = (s, i, baseCuboidId) :: build_plan // use the base cuboid for all cuboids
        pi.step
    }
    if (showProgress) println
    build_plan.reverse
  }
}

object AlwaysBaseCuboidBuilderST extends SingleThreadedCubeBuilder with AlwaysBaseCuboidBuilder

object AlwaysBaseCuboidBuilderMT extends MultiThreadedCubeBuilder with AlwaysBaseCuboidBuilder