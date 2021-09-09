package core.solver

import core._
import util._

class SliceSparseSolver[T](
                            nb: Int,
                            bs: collection.mutable.ArrayBuffer[Interval[T]],
                            ps: List[List[Int]],
                            vs: Seq[T],
                            sf: Int => Boolean = _ => true,
                          )(implicit num: Fractional[T]) extends SparseSolver[T](nb, bs, Nil, Nil, sf) {

  var count = 0
  val minVals = collection.mutable.Map[Int, T]()
  minVals ++= (0 until n_vars).map(_ -> num.zero)
  val maxVals = collection.mutable.Map[Int, T]() // store upper bounds for artificial constraint

  gauss(add2(ps, vs))


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
    assert(num.gteq(b, num.zero))
      eq.foreach { v =>
        if (!maxVals.isDefinedAt(v) || num.lt(b, maxVals(v)))
          maxVals(v) = b
      }
    }
    super.add(eqs)
  }

  /* Tries to updates bound and returns  true if successful */
  def my_infer_bound(x: Int, y: Int): Boolean = {
    val prof = Profiler.start(s"My infer bound")
    val row = M(x)
    /*
     s y + cj xj = B
      y = 1/s *  (B + (-cj) xj)

     max y when  min xj for cj > 0 and max xj for cj < 0 WHEN s > 0
     min y when max xj for cj > 0 and min xj for cj < 0 WHEN s > 0
    */

    val maxVal = row.data.map {
      case (`n_vars`, bi) => bi
      case (`y`, _) => num.zero
      case (j, cj) if num.gt(cj, num.zero) => num.times(num.negate(cj), minVals(j))
      case (j, cj) if num.lt(cj, num.zero) => num.times(num.negate(cj), maxVals(j))
    }.sum

    val minVal = row.data.map {
      case (`n_vars`, bi) => bi
      case (`y`, _) => num.zero
      case (j, cj) if num.gt(cj, num.zero) => num.times(num.negate(cj), maxVals(j))
      case (j, cj) if num.lt(cj, num.zero) => num.times(num.negate(cj), minVals(j))
    }.sum

    val scale = row.data(y)
    val (newMinVal, newMaxVal) = if (num.gt(scale, num.zero)) {
      (num.div(minVal, scale), num.div(maxVal, scale))
    } else {
      (num.div(maxVal, scale), num.div(minVal, scale))
    }


    assert(num.gteq(newMaxVal, newMinVal))

    var updated = false
    if(num.gt(newMinVal, minVals(y))){
      assert(num.gteq(newMinVal, num.zero))
      minVals(y) = newMinVal
      updated = true
    }
    if(num.lt(newMaxVal, maxVals(y))){
      assert(num.gteq(newMaxVal, num.zero))
      maxVals(y) = newMaxVal
      updated = true
    }
    assert(num.gteq(maxVals(y), minVals(y)))
    prof()
    updated
  }

  override protected def propagate_bounds0(vars: Seq[Int]): Seq[Int] = {
    val pi = new ProgressIndicator(det_vars.size, "PropBounds0")
    val result = (for (row <- det_vars) yield {
      val r_vars = M(row).domain.filter(k => k != n_vars && sliceFunc(k))  //only update slice vars
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
      pi.step
      res
    }).flatten.flatten.toSet.toList
    result
  }

  override def compute_bounds: Unit = {
    val all_vars = (0 until n_vars)
    val obj_vars = all_vars.filter(sliceFunc)
    println(s"Computing bounds for ${
      obj_vars.length
    }/$n_vars")

    if (df > 30) {
      val p1 = Profiler.start("Super Prop Bounds")
      super.propagate_bounds(all_vars) //use all vars bounds as input.
      p1()
    } else if (df > 0) {
      val p2 = Profiler.start("Simplex Bounds")
      simplex_bounds(obj_vars)
      p2()
    } else {
      val p3 = Profiler.start("Linear Bounds")
      linear_propagate_bounds()
      p3()
    }
    val p4 = Profiler.start("To Interval Bounds")
    (0 until n_vars).foreach { v =>
      bounds(v) = Interval(Some(minVals(v)), Some(maxVals(v)))
    }
    p4()

    val p5 = Profiler.start("Add points")
    add_points((0 until n_vars))
    p5()

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
    Nil  //I believe the result is used no where
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

      val prof = Profiler.start(s"RunSimplex max=$maximize")

      val initProf = Profiler.start(s"Simplex Initialization")
      val simplex = new DualSimplex[T](n_vars, det_vars, constraints)
      //if (df > 30)
      simplex.iter_limit = 20
      initProf()
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
      prof()
    }


    val simplex_obj_vars = if (vars.length > df) {
      free_vars //Run simplex on free vars and then linear_bounds to determine rest
    } else {
      vars
    }

    val pi = new ProgressIndicator(vars.length, "Simplex")
    val minP = Profiler.start("RunSimplex")
    for (i <- simplex_obj_vars) yield {
      run_my_simplex(i, false)
      run_my_simplex(i, true)
      pi.step
    }
    minP()
    if (vars.length > df)
      linear_propagate_bounds()

  }
}
