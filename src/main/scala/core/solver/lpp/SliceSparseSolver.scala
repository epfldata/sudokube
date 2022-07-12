package core.solver.lpp

import core._
import util._

/** Alternative to SparseSolver originally intended to be optimized for solving a subset of variables in presence of slice
 * but contains other optimizations as well
 * */
class SliceSparseSolver[T](
                            nb: Int,
                            bs: collection.mutable.ArrayBuffer[Interval[T]],
                            ps: Seq[Seq[Int]],
                            vs: Seq[T],
                            sf: Int => Boolean = _ => true,
                          )(implicit num: Fractional[T]) extends SparseSolver[T](nb, bs, Nil, Nil, sf) {

  var count = 0
  val minVals = collection.mutable.Map[Int, T]()
  minVals ++= (0 until n_vars).map(_ -> num.zero)
  val maxVals = collection.mutable.Map[Int, T]() // store upper bounds for artificial constraint

  val maxFetchedVars = collection.mutable.BitSet()
  gauss(add2(ps, vs)) //gauss also called by superclass that's why we pass Nil to super constructor


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

  override def gauss(pivs: Seq[Int]) {
    //val pi = new ProgressIndicator(pivs.length, "Gauss")
    for (piv <- pivs.sorted.reverse) {
      val pivot_row = M(piv)

      if (pivot_row(piv) != num.one)
        println(piv + " " + pivot_row(piv) + "\n " + M)
      assert(pivot_row(piv) == num.one)

      for (other_row <- piv + 1 to n_vars - 1) {

        M.data(other_row) = M.data(other_row) match {
          case None => None
          case Some(or) => {
            val o: T = or(piv)

            if (o == num.zero) Some(or)
            else {
              val newrow = or + (pivot_row * num.negate(o))
              if (newrow.data.size <= 2) {
                val newkeys = newrow.data.filterKeys(_ != n_vars)
                if (newkeys.size == 1) {
                  val (newvar, coeff) = newkeys.head
                  solved_vars += newvar
                  val newval = num.div(newrow(n_vars), coeff)
                  minVals(newvar) = newval
                  maxVals(newvar) = newval
                }
              }
              Some(newrow)
            }
          }
        }
      }
      //pi.step
    }
  }


  override def add(eqs: Seq[Eq_T]): Seq[Int] = {
    val old_n_det_vars = n_det_vars
    var new_pivots = List[Int]()

    for ((vars, total) <- eqs) {
      val lv = vars.last
      assert(lv == vars.max)

      vars.foreach { v =>
        if (!maxVals.isDefinedAt(v) || num.lt(total, maxVals(v))) {
          maxVals(v) = total
        }
      }

      if (total == num.zero) {
        solved_vars ++= vars
        vars.foreach { v =>
          if (M.data(v) == None) { //Do not replace existing equation
            M.data(v) = Some(SparseRow(n_vars + 1, Map(v -> num.one)))
            n_det_vars += 1
            new_pivots = v :: new_pivots
          }
        }
      } else if (!maxFetchedVars.contains(lv)) { //Replace eqn even if we have the solver solution
        maxFetchedVars += lv
        n_det_vars += M.data(lv).map(_ => 0).getOrElse(1)
        val l = List((n_vars, total)) ++ vars.map((_, num.one))
        M.data(lv) = Some(SparseRow[T](n_vars + 1, l.toMap))
        new_pivots = lv :: new_pivots
      }
    }

    //println((n_det_vars - old_n_det_vars) + " rows (out of "
    //  + eqs.length + ") added (#df=" + df
    //  + ", #det=" + n_det_vars
    //  + ", #solved=" + solved_vars.size
    //  + ", cum.span=" + cumulative_interval_span + ")")
    new_pivots
  }

  /**
   * Pre-emptively checks if there is any new independent equation by fetching projection with bits dims
   * */
  override def shouldFetch(dims: Seq[Int]): Boolean = {
    val new_basis_vars = Bits.max_group_values(dims, 0 until n_bits)
    new_basis_vars.foldLeft(false)((acc, cur) => acc || !maxFetchedVars.contains(cur)) //at least one new basis var
  }

  /* Tries to updates bound of variable x using row r and returns  true if successful */
  def my_infer_bound(r: Int, x: Int): Boolean = {
    Profiler.noprofile(s"My infer bound") {
      val row = M(r)
      /*
    s y + cj xj = B
     y = 1/s *  (B + (-cj) xj)

    max y when  min xj for cj > 0 and max xj for cj < 0 WHEN s > 0
    min y when max xj for cj > 0 and min xj for cj < 0 WHEN s > 0
   */

      val maxVal = row.data.map {
        case (`n_vars`, bi) => bi
        case (`x`, _) => num.zero
        case (j, cj) if num.gt(cj, num.zero) => num.times(num.negate(cj), minVals(j))
        case (j, cj) if num.lt(cj, num.zero) => num.times(num.negate(cj), maxVals(j))
      }.sum

      val minVal = row.data.map {
        case (`n_vars`, bi) => bi
        case (`x`, _) => num.zero
        case (j, cj) if num.gt(cj, num.zero) => num.times(num.negate(cj), maxVals(j))
        case (j, cj) if num.lt(cj, num.zero) => num.times(num.negate(cj), minVals(j))
      }.sum

      val scale = row.data(x)
      val (newMinVal, newMaxVal) = if (num.gt(scale, num.zero)) {
        (num.div(minVal, scale), num.div(maxVal, scale))
      } else {
        (num.div(maxVal, scale), num.div(minVal, scale))
      }


      assert(num.gteq(newMaxVal, newMinVal))

      var updated = false
      if (num.gt(newMinVal, minVals(x))) {
        assert(num.gteq(newMinVal, num.zero))
        minVals(x) = newMinVal
        updated = true
      }
      if (num.lt(newMaxVal, maxVals(x))) {
        assert(num.gteq(newMaxVal, num.zero))
        maxVals(x) = newMaxVal
        updated = true
      }
      assert(num.gteq(maxVals(x), minVals(x)))
      updated
    }
  }

  override protected def propagate_bounds0(vars: Seq[Int]): Seq[Int] = {
    //val pi = new ProgressIndicator(det_vars.size, "PropBounds0")
    val result = (for (row <- det_vars) yield {
      val r_vars = M(row).domain.filter(k => k != n_vars && sliceFunc(k)) //only update slice vars
      val i = r_vars.intersect(vars)
      val res = if (i.size == 0) None
      else Some((for (v <- r_vars) yield {
        if ((r_vars.length == 1) || (i.size > 1) || (!i.contains(v))) {
          if (my_infer_bound(row, v))
            Some(v)
          else None
          //if (r_vars.length == 1) solved_vars = solved_vars + v
          //if (old_bounds != bounds(v)) Some(v) else None
        }
        else None
      }).flatten)
      //pi.step
      res
    }).flatten.flatten.toSet.toList
    result
  }

  override def compute_bounds: Unit = {
    val all_vars = (0 until n_vars)
    val unsolved_vars = all_vars.filter(!solved_vars.contains(_))
    val obj_vars = unsolved_vars.filter(sliceFunc)
    //println(s"Computing bounds for ${
    //  obj_vars.length
    //}/$n_vars")

    if (df > 30) {
      val p1 = Profiler.noprofile("Super Prop Bounds") {
        super.propagate_bounds(unsolved_vars) //TODO: Fix input
      }
    } else if (df > 0) {
      val p2 = Profiler.noprofile("Simplex Bounds") {
        simplex_bounds(obj_vars)
      }
    } else {
      val p3 = Profiler.noprofile("Linear Bounds") {
        linear_propagate_bounds()
      }
    }

    all_vars.foreach { v =>
      bounds(v) = Interval(Some(minVals(v)), Some(maxVals(v)))
    }

    add_points(unsolved_vars)

  }

  /**
   * Assumes bounds on non-basic variables have been computed
   * Computes bounds on basic variables using those of non-basic variables
   * Also works when df = 0
   */
  def linear_propagate_bounds() = {
    det_vars.filter(sliceFunc).foreach { x =>
      my_infer_bound(x, x)
    }
  }


  override def my_bounds(vars: Seq[Int]): Seq[Int] = {
    simplex_bounds(vars)
    Nil //I believe the result is used no where
  }

  def simplex_bounds(vars: Seq[Int]): Unit = {
    count += 1
    val iterations = collection.mutable.ArrayBuffer[Int]()

    //val basis_Vars = (0 until n_vars).map(sliceFunc).take(n_det_vars)


    //TODO: Choose initial basis according to slice
    //rewrite constraint so that constants appear first
    val constraints = M.data.toList.flatten.map {
      row =>
        SparseRow(n_vars + 1, row.data.map {
          case (`n_vars`, v) => (0, v)
          case (j, v) => (j + 1, v)
        })
    }

    //CANNOT RESUME SIMPLEX ON SAME INSTANCE DUE TO ARTIFICIAL CONSTRAINT

    /**
     * Starts a new instance of simplex to find bounds of {{{obj_var}}}
     * Constraints are added from Gaussian eliminated equations
     * Maximum bounds are also passed to create artificial constraints if necessary
     */
    def run_my_simplex(obj_var: Int, maximize: Boolean) {


      val simplex = Profiler.noprofile(s"Simplex Initialization") {
        new DualSimplex[T](n_vars, det_vars, constraints)
      }
      //if (df > 30)
      //simplex.iter_limit = 20
      simplex.maxVals = maxVals
      simplex.minVals = minVals
      simplex.set_simple_objective(obj_var, maximize)
      assert(simplex.isOptimal)

      //always run dual_algorithm
      simplex.run(obj_var, maximize)

      //println("Optimum value is " + optval)
      iterations += simplex.it_cnt
      //println(s"Simplex constraints = ${simplex.n_constraints}  orig_eqns = ${n_det_vars}  vars = ${simplex.n_vars}  orig_vars = ${n_vars} free = ${simplex.freeRows},  total freed = ${simplex.totalFreed}")
      //println(s"Current basis =" + simplex.base_v.mkString(" "))
    }


    val simplex_obj_vars = vars

    /*
      Running simplex only on free variables doesn't work. They need to be simultaneously max or min to determine tightest
      bounds on the det_vars. This can be a fast approximation however.
      TODO: Run parallel objective when vars.length > df
     */
    //val simplex_obj_vars = if (vars.length > df) {
    //  free_vars //Run simplex on free vars and then linear_bounds to determine rest
    //} else {
    //  vars
    //}

    val pi = new ProgressIndicator(vars.length, "Simplex")
    Profiler.noprofile("RunSimplex") {
      for (i <- simplex_obj_vars) yield {
        run_my_simplex(i, false)
        run_my_simplex(i, true)
        pi.step
      }
    }
    //if (vars.length > df)
    //  linear_propagate_bounds()

  }
}
