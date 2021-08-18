//package ch.epfl.data.sudokube
package planning
import util._


abstract class PlanBuilder {
  def result: Plan
}


/** A Selinger-style dynamic programming cost-based optimizer. Finds an
    optimal plan but is rather expensive.
*/
case class CostBasedOptimizer(
  n_bits: Int,
  projections: List[ProjectionMetaData]
) extends PlanBuilder {
  var plans = (for(proj <- projections) yield Plan(n_bits, List(proj))).toList

  println(plans.length + " initial plans built.")

  var num_dtests = 0
  var num_alive = 1
  while(num_alive > 0) {
    num_alive = 0
    var kills = 0

    for(plan <- plans) if(plan.alive) {
      plan.alive = false // (plan.df == 0)
      print("*"); Console.flush
      for(proj <- projections) {
        if(! plan.dominated_projections.contains(proj)) {
          val new_plan = plan.mk_copy()
          new_plan.add_step(proj)
          num_dtests += plans.length
          if(!plans.exists(x => x.dominates(new_plan))) {
            kills += plans.length
            num_dtests += plans.length
            val relevant_plans = plans.filter(x => ! new_plan.dominates(x))
            kills -= relevant_plans.length
            plans = new_plan :: relevant_plans
            new_plan.alive = true
            num_alive += 1
          }
        }
      }
    }
    println(" (+" + num_alive + " -" + kills + ")")
  }

  println(plans.length + " built, " +
          plans.filter(_.df == 0).length + " complete.")
  println(num_dtests + " domination tests.")

  val result = plans.filter(_.df == 0)(0)
}


case class NaiveOptimizer(
  q: Int,
  p: List[ProjectionMetaData]
) extends PlanBuilder {

  private def super_order(p1: ProjectionMetaData, p2: ProjectionMetaData) = {
    if(p1.cost_factor == p2.cost_factor) {
      if(p1.accessible_bits.length == p2.accessible_bits.length) {
        Util.lists_lt(p1.accessible_bits, p2.accessible_bits)
      }
      else p1.accessible_bits.length < p2.accessible_bits.length
    }
    else p1.cost_factor < p2.cost_factor
  }

  // This assumes that cuboids do not need to be read completely.
  // If we always read cuboids completely, it would be better to order
  // them by increasing size.
  // Filters out dominated maps of same cost_factor. Again, this avoids
  // Redundant work in case we are only interested in a complete result,
  // but it is not ideal in case we want to do online aggregation.
  private val o: List[ProjectionMetaData] =
    p.groupBy(_.cost_factor).mapValues(
      x => x.filter(y =>
        ! x.exists(z => z.dominates(y)))).toList.map(_._2).flatten

  private val o2 = o.sortWith(super_order)
  val result = Plan(q, o2)
}


/** Isn't online but builds a plan for online aggregation.
    Creates a particular sort order; does not drop any projections from use.

    TODO: drop really small cuboids subsumed by others in the plan that
    aren't too large, so would be done fetching soon.

*/
case class OnlinePlanBuilder(
  q: Int,
  p: List[ProjectionMetaData]
) extends PlanBuilder {

  // do the cheap ones first
  val result = Plan(q, p.sortBy(_.mask.length))
}


/** Finds the smallest cuboid that subsumes the query and builds a plan
    just from it. This is closest to the classical approach to query evaluation
    in data cubes.
*/
case class BestSubsumerPlanBuilder(
  q: Int,
  qprojections: List[ProjectionMetaData]
) extends PlanBuilder {

  // this is a singleton list with the item in it.
  private val best_subsumer : List[ProjectionMetaData] =
    (qprojections.groupBy(_.accessible_bits.length))(q)

  val result = Plan(q, best_subsumer)
}


