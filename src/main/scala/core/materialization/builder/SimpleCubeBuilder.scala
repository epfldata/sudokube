package core.materialization.builder

import core.materialization.MaterializationScheme
import util.ProgressIndicator

object SimpleCubeBuilder extends SingleThreadedCubeBuilder {

  /** Create a plan for building each cuboid from the smallest that subsumes
   * it. Using BestSubsumerPlanBuilder for this is too expensive though.
   *
   * The resulting build plan starts with the full cube and ends at the
   * zero-dimensional cube. One can iterate through it from begin to end
   * and dependencies will always be ready.
   *
   * The result is a list of triples (a, b, c), where
   * a is the set of dimensions of the projection,
   * b is the key by which the cuboid is to be indexed (which is also
   * its index in projections), and
   * c is the key of the cuboid from which it is to be built.
   * The full cube will have -1 in field c.
   *
   * Note: the build plan construction is deterministic. Given the
   * same projections (in the same order), it always produces the same
   * result.
   */
  override def create_build_plan(m: MaterializationScheme, showProgress: Boolean): List[(Set[Int], Int, Int)] = {
    // aren't they sorted by length by construction?
    val ps = m.projections.zipWithIndex.sortBy(_._1.length).reverse.toList
    assert(ps.head._1.length == m.n_bits)

    // the edges (_2, _3) form a tree rooted at the full cube
    var build_plan: List[(Set[Int], Int, Int)] =
      List((ps.head._1.toSet, ps.head._2, -1))

    val pi = new ProgressIndicator(ps.tail.length, "Create Build Plan", showProgress)

    ps.tail.foreach {
      case ((l: List[Int]), (i: Int)) => {
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
