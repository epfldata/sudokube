package core.solver

import core.{Interval, SimplexAlgo, SolverTools, SparseMatrix, SparseRow, SparseSolver}
import util.{Bits, ProgressIndicator}

class SliceSparseSolver[T](
                          nb: Int,
                          bs: collection.mutable.ArrayBuffer[Interval[T]],
                          ps: List[List[Int]],
                          vs: Seq[T],
                          sf: Int => Boolean = _ => true,
                        )(implicit num: Fractional[T]) extends SparseSolver[T](nb, bs, ps, vs, sf) {


  override def simplex_add(): Seq[Int] = {
    //do nothing
    Nil
  }
  override protected def full_matrix_simplex(vars: List[Int]): Unit = {
    //do nothing
  }
  override protected def infer_bound(row: Int, v: Int): Interval[T] = {
    Interval(None, None)
  }

  override def mk_tableau: SimplexAlgo[T] = {
    null
  }

  override def propagate_bounds(vars: Seq[Int]): Unit = {
  }

  override def add(eqs: Seq[(Seq[Int], T)]): Seq[Int] = {
   //Additionally maintain upper bounds
    eqs.foreach { case (eq, b) =>
      eq.foreach { v =>
        val cur = bounds(v)
        if (!cur.ub.isDefined || num.lt(b, cur.ub.get))
          bounds(v) = Interval(cur.lb, Some(b))
      }
    }
    super.add(eqs)
  }

  override def compute_bounds: Unit = {
    val obj_vars = (0 until n_vars).filter(sliceFunc)
    println(s"Computing bounds for ${
      obj_vars.length
    }/$n_vars")
    my_bounds(obj_vars)
  }

  var count = 0

  override def my_bounds(vars: Seq[Int]) = {
    count += 1
    val iterations = collection.mutable.ArrayBuffer[Int]()

    /**
     * Starts a new instance of simplex to find bounds of {{{obj_var}}}
     * Constraints are added from Gaussian eliminated equations
     * Maximum bounds are also passed to create artificial constraints if necessary
     */
    def run_my_simplex(obj_var: Int, maximize: Boolean): Option[T] = {
      val constraints = M.data.toList.flatten.map {
        row => (row.data.toList.filter(l => l._1 != n_vars), row(n_vars))
      }

      val simplex = new DualSimplex[T](n_vars, det_vars, constraints)

      val maxVals = (0 until n_vars).map { case v => v -> bounds(v).ub.get }.toMap
      simplex.maxVals ++= maxVals

      simplex.set_simple_objective(obj_var, maximize)
      assert(simplex.isOptimal)

      //always run dual_algorithm
      val optval =
        simplex.dual_algo match {
          case Some(result) => Some(if (!maximize) result else num.negate(result))
          case None => None
        }

      //println("Optimum value is " + optval)
      iterations += simplex.it_cnt
      optval
    }

    val pi = new ProgressIndicator(vars.length, "Simplex")
    for (i <- vars) yield {
      val intv = Interval(run_my_simplex(i, false), run_my_simplex(i, true))
      bounds(i) = intv
      pi.step
    }
    println(s"Simplex round $count #iterations = " + iterations.mkString(" "))
    add_points(vars)
  }
}
