//package ch.epfl.data.sudokube
package core


/** Differently from Solver, SparseSolver uses our own sparse matrix
    representation SparseMatrix[T], rather than the breeze dense matrix
    implementation. T can be Rational, so we have full precision.

    It also uses our own simplex solver, though not in simplex_add
    and full_matrix_simplex.

    The constructor does Gaussian elimination but no further solving.

    The recommended use is to call compute_bounds.

    {{{
    import core._
    import RationalTools._
    val s = SparseSolver[Rational](3, SolverTools.mk_all_non_neg(8), List(List(0,1), List(2)), List(2,2,2,2,4,4))
    scala> s.M
    res0: core.SparseMatrix[core.Rational] =
    1.0   1.0   1.0   1.0  0.0  0.0  0.0  0.0  4.0
    1.0   0.0   0.0   0.0  1.0  0.0  0.0  0.0  2.0
    0.0   1.0   0.0   0.0  0.0  1.0  0.0  0.0  2.0
    0.0   0.0   1.0   0.0  0.0  0.0  1.0  0.0  2.0
    -1.0  -1.0  -1.0  0.0  0.0  0.0  0.0  1.0  -2.0
    }}}
*/
case class SparseSolver[T](
  val n_bits: Int,
  bounds: collection.mutable.ArrayBuffer[Interval[T]],
  private val projections: List[List[Int]],
  private val v: Seq[T]
)(implicit num: Fractional[T]) {

  assert(n_bits < 31)
  val n_vars = 1 << n_bits
  assert(bounds.length == n_vars)

  val M = SparseMatrix[T](n_vars, n_vars + 1)
  protected var n_det_vars = 0

  def df = n_vars - n_det_vars   // remaining degrees of freedom
  protected def free_vars = (0 to n_vars - 1).filter(x => M.data(x) == None)
  def det_vars  = (0 to n_vars - 1).filter(x => M.data(x) != None)
  var solved_vars = Set[Int]()

  /** Gaussian elimination.
      This implementation is intentionally limited in that it requires that
      the pivot fields have value one. That doesn't make the algorithm
      simpler, but it's a guaranteed property.
  */
  def gauss(pivs: Seq[Int]) {
    for(piv <- pivs.sorted.reverse) {
      val pivot_row = M(piv)

      assert(pivot_row(piv) == num.one)

      for(other_row <- piv + 1 to n_vars - 1) {

        M.data(other_row) = M.data(other_row) match {
          case None     => None
          case Some(or) => {
            val o: T = or(piv)

            if(o == num.zero) Some(or)
            else Some(or + (pivot_row * num.negate(o)))
          }
        }
      }
    }
  }

  /** A very unsophisticated measure of how far off the solution we still
      are. For termination conditions.
  */
  def cumulative_interval_span : Option[T] = {
    val l = bounds.toList.map(_.span)
    if(l.contains(None)) None
    else Some(l.flatten.sum)
  }

  type Eq_T = (Seq[Int], T)

  def add(eqs: Seq[Eq_T]) : Seq[Int] = {
    val old_n_det_vars = n_det_vars
    var new_pivots = List[Int]()

    for((vars, total) <- eqs) {
      val lv = vars.last         // last var can be used to characterize
                                 // linear dependance
      assert(lv == vars.max)

      if (M.data(lv) == None) {
        val l = List((n_vars, total)) ++ vars.map((_, num.one))
        M.data(lv) = Some(SparseRow[T](n_vars + 1, l.toMap))
        new_pivots = lv :: new_pivots
        n_det_vars += 1
      }
    }

    println((n_det_vars - old_n_det_vars) + " rows (out of "
          + eqs.length + ") added (#df=" + df
          + ", #solved=" + solved_vars.size
          + ", cum.span=" + cumulative_interval_span + ").")
    new_pivots
  }

  /** used by the constructor to build the initial matrix from
      projections and v.
      Also used in DataCube.online_agg().
  */
  def add2(a: Seq[List[Int]], b: Seq[T]) : Seq[Int] =
    add(SolverTools.mk_constraints(n_bits, a, b))

  // We do gaussian elimination in the constructor.
  gauss(add2(projections, v))

  /** returns which new equations for solved variables were added.
      This is a subset of the input sequence vars.
  */
  protected def add_points(
    vars: Seq[Int] = (0 to n_vars - 1).toList
  ) : Seq[Int] = {

    val point_intervals = vars.filter(v => bounds(v).isPoint)

    val new_eqs : Seq[Eq_T] = point_intervals.map(v => {
      solved_vars = solved_vars + v
      (Vector(v), bounds(v).lb.get)
    })

    if(! new_eqs.isEmpty) {
      val new_pivots = add(new_eqs)
      assert(new_pivots.toSet.subsetOf(vars.toSet))
      gauss(new_pivots)
      new_pivots
    }
    else List[Int]()
  }

  /** the matrix needs to be gaussed first.
      Returns which variables were newly solved.
  */
  def simplex_add() : Seq[Int] = {
    // project matrix and bounds down to free variables
    val M2 = M.select_cols(free_vars ++ List(n_vars))
    val bounds0 = util.Util.filterIndexes(bounds, free_vars)
    val objectives = (0 to df - 1).map(x => List((num.one, x)))

    val new_bounds = DenseSolverTools.simplex[T](M2, bounds0, objectives, true)

    free_vars.zip(new_bounds).foreach {
      case(v, i) => bounds(v) = bounds(v).intersect(i)
    }

    add_points(free_vars)
  }

  /** compute bounds for variables `vars' using the Apache simplex
      solver.
  */
  protected def full_matrix_simplex(vars: List[Int]) = {
    val objectives = vars.map(x => List((num.one, x)))
          
    val new_bounds = DenseSolverTools.simplex[T](M, bounds, objectives, false)
      
     vars.zip(new_bounds).foreach {
       case (v, i) => bounds(v) = bounds(v).intersect(i)
     }
  }


  /**
  Infers bounds based on the bounds on the other values (and the
  other values *only*, so the resulting bounds still need to be intersected
  with the input bounds) in a row/sum.

  Example: A one-dimensional cuboid of uncertain values (with intervals
  x1: [1,2] and x2: [3,6]) gets projected down to zero dimensions,
  and the sum x1+x2 must be 4.
  infer_bounds determines that x1, x2 must
  be in the intervals [-2, 1] and [2,3], respectively.
  Note that this method does not
  intersect with the input intervals yet, otherwise we could determine the
  cuboid to have the exact values 1 and 3.
  {{{
    import util.SloppyFractionalInt._
    val bounds = SolverTools.mk_all_non_neg[Int](1 << 1)
    bounds(0) = Interval(Some(1), Some(2))
    bounds(1) = Interval(Some(3), Some(6)) 
    val s = SparseSolver[Int](1, bounds, List(List()), List(4))
    s.M(1) == SparseRow(3,Map(2 -> 4, 0 -> 1, 1 -> 1))
    s.infer_bound(1, 0) == Interval(Some(-2),Some(1))
    s.infer_bound(1, 1) == Interval(Some(2),Some(3))
  }}}
  For x1, the computation is,
    for the lower bound, 4 - 6, where 6 is the upper bound on x2, and
    for the upper bound, 4 - 3, where 3 is the lower bound on x2.
  */
  protected def infer_bound(row: Int, v: Int) : Interval[T] = {
    // println("infer_bound(" + row + ", " + v + ")")

    val b  = M(row)(n_vars)
    val other_vars = M(row).domain.filter(x => ((x != v) && (x != n_vars)))
    val l          = other_vars.map(w => bounds(w) * M(row)(w))
    val s          = try { IntervalTools.sum(l)
    } catch { case e: Exception => {
      println("infer_bound(" + row + ", " + v + ") crashed")
      println("bounds = " + bounds)
      println("row: " + M(row))
      // println("M:\n" + M.select_cols(free_vars))

      val abc = (for(r <- M.data.filter(_ != None)) yield {
        r.get.data.toList.filter{ case (v, _) => v != n_vars }.map {
          case (_, x) => (num.toInt(x), 1)}
      }).flatten.groupBy(x => x._1).mapValues(x => x.map(_._2).sum)
      println("Matrix weight distribution: " + abc)

      println(l)
      assert(false)
      IntervalTools.sum(l)
    }}

    assert(M(row)(v) != num.zero)
    val scaling_factor : T = num.div(num.one, M(row)(v))

    val minus1 : T         = num.negate(num.one)  // == -1

    (IntervalTools.point(b) + (s * minus1)) * scaling_factor
  }

  /** Propagates bounds row by row.
      Returns variables whose bounds got updated. One may have to
      call propagate_bounds again until a fixpoint is reached.

      @param vars  the variables that have new bounds that are to be
                   propagated.
  */ 
  protected def propagate_bounds0(vars: Seq[Int]) : Seq[Int] = {

    val result = (for(row <- det_vars) yield {
      val r_vars = M(row).domain.filter(_ != n_vars)
      val i = r_vars.intersect(vars)
      if(i.size == 0) None
      else Some((for(v <- r_vars) yield {
        if((r_vars.length == 1) || (i.size > 1) || (! i.contains(v))) {

          val old_bounds = bounds(v)
          bounds(v) = bounds(v).intersect(infer_bound(row, v))
          if(r_vars.length == 1) solved_vars = solved_vars + v
          if(old_bounds != bounds(v)) Some(v) else None
        }
        else None
      }).flatten)
    }).flatten.flatten.toSet.toList

    add_points(result)
    result
  }

  /** at most three iterations. This doesn't seem to terminate by itself
      in general.
  */
  def propagate_bounds(vars: Seq[Int]) {
    var todo = vars.toList
    var i = 0
    while((! todo.isEmpty) && (i < 3)) {
      print("&")
      todo = propagate_bounds0(todo).toList
      i += 1
   }
   println("End of propagate_bounds; (#df=" + df
          + ", #solved=" + solved_vars.size
          + ", cum.span=" + cumulative_interval_span + ").")
  }

  def mk_tableau : SimplexAlgo[T] = {

    val eqs : List[(List[(Int, T)], T)] =
      M.data.toList.flatten.map(
        row => (row.data.toList.filter(l => l._1 != n_vars), row(n_vars)))

    SimplexAlgo_Aux.mk_tableau[T](n_vars, det_vars, eqs)
  }

  def my_bounds(vars: Seq[Int]) = {
    def run_my_simplex(obj_var: Int, maximize: Boolean) : Option[T] = {
      val a = mk_tableau
      a.set_simple_objective(obj_var, maximize)
      a.algo match {
        case Some(result) => Some(if(maximize) result else num.negate(result))
        case None => None
      }
    }

    for(i <- vars) yield {
      val intv = Interval(run_my_simplex(i, false), run_my_simplex(i, true))
      bounds(i) = bounds(i).intersect(intv)
    }
    println // a.algo wrote a single unterminated line
    add_points(vars)
  }

  def compute_bounds {
    // even if df == 0, we initially haven't restricted the bounds or
    // set the solved vars: call propagate_bounds for this.
    propagate_bounds(0 to n_vars - 1)

    if((df > 0) && (df < 30)) {
/*
      try   { simplex_add }
      catch { case e: Exception => println("Apache Simplex is buggy 1.") }

      propagate_bounds(0 to n_vars - 1)

      if((df != 0) && (det_vars.size <= 20)) {
        try   { full_matrix_simplex(det_vars.toList) }
        catch { case e: Exception => println("Apache Simplex is buggy 2.") }
      }
*/

      val unsolved_vars = (0 to n_vars - 1).filter(! solved_vars.contains(_))
      my_bounds(unsolved_vars)
      //my_bounds(free_vars)
      //propagate_bounds(free_vars)

    } else {
      println("#df = " + df + ", skipping optimization.")
    }
  }
}


/*
// this is an instance that takes relatively many iterations for simplex.

import core._
import RationalTools._
val v = Array(39, 18, 25, 31, 29, 43, 28, 35, 30, 22, 11, 21, 10, 15, 30, 49, 28, 40, 44, 54, 81, 48, 53, 88, 38, 65, 30, 33, 75, 70, 54, 71, 65, 61, 50, 61, 64, 58, 27, 50, 31, 72, 13, 50, 90, 55, 68, 57, 22, 42, 74, 99, 46, 56, 55, 42, 69, 57, 55, 56, 52, 70, 26, 51, 53, 73, 43, 68, 60, 62, 41, 36, 57, 46, 15, 48, 72, 73, 62, 63, 26, 46, 18, 76, 82, 52, 76, 60, 60, 55, 64, 58, 48, 43, 30, 78
).map(Rational(_, 1))

val l = List(List(0, 1, 2, 3), List(1, 2, 4), List(2, 3, 4), List(1, 3, 5), List(0, 3, 4), List(2, 4, 5), List(0, 3, 5), List(2, 3, 5), List(1, 3, 4), List(0, 1, 4), List(0, 1, 5))

val bounds = SolverTools.mk_all_non_neg[Rational](64)
val s = SparseSolver[Rational](6, bounds, l, v)
s.my_bounds(0 to 63)
val a = s.mk_tableau
a.set_simple_objective(5, true) // 59 iterations
assert(a.algo == Some(Rational(119, 3)))
*/

/*
// NOTE: The matrix isn't always the identity matrix when #df == 0

scala> dc.online_agg[Rational](List(0,1,2), 2)
prepare = List(List(0, 1, 2), List(1, 2), List(0, 2), List(0, 1))
0 rows (out of 0) added (#df=8, #solved=0, cum.span=None).
List(1, 2)
4 rows (out of 4) added (#df=4, #solved=0, cum.span=None).
List(0, 2)
2 rows (out of 4) added (#df=2, #solved=0, cum.span=Some(54)).
1 rows (out of 3) added (#df=1, #solved=3, cum.span=Some(32)).
0 rows (out of 1) added (#df=1, #solved=4, cum.span=Some(16)).
1.0  0.0  0.0  0.0  0.0   0.0  0.0  0.0  15.0  
0.0  1.0  0.0  0.0  0.0   0.0  0.0  0.0  0.0   
0.0  0.0  1.0  0.0  0.0   0.0  0.0  0.0  3.0   
0.0  0.0  0.0  1.0  0.0   0.0  0.0  0.0  0.0   
0.0  0.0  0.0  0.0  1.0   1.0  0.0  0.0  5.0   
0.0  0.0  0.0  0.0  1.0   0.0  1.0  0.0  4.0   
0.0  0.0  0.0  0.0  -1.0  0.0  0.0  1.0  0.0   
ArrayBuffer(Interval(Some(15),Some(15)), Interval(Some(0),Some(0)), Interval(Some(3),Some(3)), Interval(Some(0),Some(0)), Interval(Some(0),Some(4)), Interval(Some(1),Some(5)), Interval(Some(0),Some(4)), Interval(Some(0),Some(4)))
List(0, 1)
1 rows (out of 4) added (#df=0, #solved=4, cum.span=Some(16)).
0 rows (out of 4) added (#df=0, #solved=8, cum.span=Some(0)).
#df = 0, skipping optimization.
1.0   0.0  0.0  0.0  0.0  0.0  0.0  0.0  15.0   
0.0   1.0  0.0  0.0  0.0  0.0  0.0  0.0  0.0    
0.0   0.0  1.0  0.0  0.0  0.0  0.0  0.0  3.0    
0.0   0.0  0.0  1.0  0.0  0.0  0.0  0.0  0.0    
1.0   0.0  0.0  0.0  1.0  0.0  0.0  0.0  15.0   
-1.0  0.0  0.0  0.0  0.0  1.0  0.0  0.0  -10.0  
-1.0  0.0  0.0  0.0  0.0  0.0  1.0  0.0  -11.0  
1.0   0.0  0.0  0.0  0.0  0.0  0.0  1.0  15.0   
ArrayBuffer(Interval(Some(15),Some(15)), Interval(Some(0),Some(0)), Interval(Some(3),Some(3)), Interval(Some(0),Some(0)), Interval(Some(0),Some(0)), Interval(Some(5),Some(5)), Interval(Some(4),Some(4)), Interval(Some(0),Some(0)))
*/

