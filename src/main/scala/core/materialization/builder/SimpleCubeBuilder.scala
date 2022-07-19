package core.materialization.builder

import core.materialization.MaterializationScheme
import util.ProgressIndicator

/**
 *  Searches among all currently materialized cuboids to determine what to project
 */
trait SimpleBuildPlan {


   def create_build_plan(m: MaterializationScheme, showProgress: Boolean = false): List[(Set[Int], Int, Int)] = {
    // aren't they sorted by length by construction?
    val ps = m.projections.zipWithIndex.sortBy(_._1.length).reverse.toList
    assert(ps.head._1.length == m.n_bits)

    // the edges (_2, _3) form a tree rooted at the full cube
    var build_plan: List[(Set[Int], Int, Int)] =
      List((ps.head._1.toSet, ps.head._2, -1))

    val pi = new ProgressIndicator(ps.tail.length, "Create Simple Build Plan", showProgress)

    ps.tail.foreach {
      case ((l: IndexedSeq[Int]), (i: Int)) => {
        val s = l.toSet

        // first match is cheapest
        val y = build_plan.find { case (s2, _, _) => s.subsetOf(s2) }
        y match {
          case Some((_, j, _)) => build_plan = (s, i, j) :: build_plan
          case None => assert(false)
        }
        pi.step
      }
    }
    if(showProgress) println
    build_plan.reverse
  }
}

object SimpleCubeBuilderST extends SingleThreadedCubeBuilder with SimpleBuildPlan
object SimpleCubeBuilderMT extends MultiThreadedCubeBuilder with SimpleBuildPlan