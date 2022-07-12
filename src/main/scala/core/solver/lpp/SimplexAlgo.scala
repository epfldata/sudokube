package core.solver.lpp

import core.solver.Rational
import util.Util._


/** Simplex algorithm for maximization. Supports >= constraints via
    artificial variables.

  {{{
  val bigM = 1000
  val constraints : List[(List[(Int, Rational)], Rational)] = List(
    (List((0, 1), (1,-1), (2, 1)       ), 2), // -x0 <= -2; x0 - x1 = 2
    (List((0, 1),                (3, 1)), 10) // x0 <= 10
  )
  val objective   : List[(Int, Rational)] = List((1, 1), (2, -bigM))

  val a = new SimplexAlgo[Rational](4, List(2,3), List(2), constraints)
  a.set_objective(objective)
  a.init_objective

scala> a.tableau
   0    1     2       3    #
Z  0.0  -1.0  1000.0  0.0  0.0
2  1.0  -1.0  1.0     0.0  2.0
3  1.0  0.0   0.0     1.0  10.0

scala>   a.init
scala> a.tableau
   0        1      2    3    #
Z  -1000.0  999.0  0.0  0.0  -2000.0
2  1.0      -1.0   1.0  0.0  2.0
3  1.0      0.0    0.0  1.0  10.0

scala> a.pivot(1,0)
scala> a.tableau
   0    1     2       3    #
Z  0.0  -1.0  1000.0  0.0  0.0
0  1.0  -1.0  1.0     0.0  2.0
3  0.0  1.0   -1.0    1.0  8.0

scala> a.pivot(2,1)
scala> a.tableau
   0    1    2      3    #
Z  0.0  0.0  999.0  1.0  8.0
0  1.0  0.0  0.0    1.0  10.0
1  0.0  1.0  -1.0   1.0  8.0

  val eqs : List[(List[(Int, Rational)], Rational)] =
    List((List((0,-1), (1, 1)), -2), (List((0,1), (2,1)), 10))

  val a = SimplexAlgo_Aux.mk_tableau(3, List(1, 2), eqs)
  a.set_simple_objective(1, true)
  a.algo
  }}}

  TODO: The initial labeling of basis vars in tableau() is broken.
        This is confusing but does not lead to incorrect optimal values.
*/
object SimplexAlgo_Aux {

  /**
    Assumptions: All vars are non-negative.
    b values can be negative; in that case we must turn the row into a
    ">="-constraint.
    Gaussian elimination has been performed.
    We use the determined vars as slack variables whenever we can.

    Create an artificial variable for:
      * determined variables that are in the objective
      * rows with a negative b
  */
  def mk_tableau[T](
    n_vars: Int,
    det_vars: Seq[Int],
    eqs : List[(List[(Int, T)], T)]
  )(implicit num: Fractional[T]) : SimplexAlgo[T] = {

    assert(det_vars == det_vars.sorted)
    val base_vars = mkAB[Int](det_vars.length, det_vars(_))

    var i = 0
    var j = 0
    val constraints = eqs.map {
      case (l, b) => {
        val x = if(num.lt(b, num.zero)) {
          // make greater-than constraint with artifical variable
          val l2 = (n_vars + i, num.one) :: (
                     l.map{ case (v, a) => (v, num.negate(a)) })
          base_vars(j) = n_vars + i

          i += 1
          (l2, num.negate(b))
        }
        else (l, b)

        j += 1
        x
      }
    }

    val artf_vars = n_vars to (n_vars + i - 1)

    new SimplexAlgo[T](n_vars + i, base_vars.toList, artf_vars, constraints)
  }
}


/**
    @param n_vars   all vars, including artificial vars
    @param _base_v  make sure the artificial vars are among them.
*/
class SimplexAlgo[T](
  n_vars: Int,
  _base_v: Seq[Int],
  artificial_vars: Seq[Int],
  constraints: List[(List[(Int, T)], T)]
)(implicit num: Fractional[T]) {

  import SparseMatrixImplicits._

  val n_constraints = constraints.length
  val M             = SparseMatrix[T](n_constraints + 1, n_vars + 1)

  /** has as many elements as constraints does. */
  /* protected */ val base_v = mkAB[Int](constraints.length, _base_v(_))

  /** Basis vars must be zero in the objective. Here we make them zero. */
  def init_objective {
    for((i, r) <- base_v.zipWithIndex)
      if(M(0)(i) != Rational(0,1)) {
        val row = get_row(i).get // this variable may only occur in one row
        assert(row == r + 1)
        M.axpy(0, row, i)
      }
  }

  /// set objective to maximize/minimize variable obj_var.
  def set_simple_objective(obj_var: Int, maximize: Boolean) {
    val bigM = num.fromInt(1000) // TODO: 1000 may be too small
    val o0 = if(maximize) num.one else num.negate(num.one)
    val objective : List[(Int, T)] =
      (obj_var, o0) ::
      (artificial_vars.map(v => (v, num.negate(bigM))).toList)

    set_objective(objective)
    init_objective
  }

  private def mk_constraint(l: List[(Int, T)], b: T) = {
    assert(num.gteq(b, num.zero))
    Some(SparseRow[T](n_vars + 1, ((n_vars, b) :: l).toMap))
  }

  // constructor / initialization.
  for(i <- 0 to n_constraints - 1) {
    val (l, b) = constraints(i)
    M.data(i + 1) = mk_constraint(l, b)
  }

  /** set_objective must be called before use. */
  def set_objective(objective: List[(Int, T)]) {
    M.data(0) = mk_constraint(objective.map(x => (x._1, num.negate(x._2))),
                num.zero)
  }

  /** picks the column for which the objective has the smallest value,
      provided it's negative (i.e., it picks the column that contains the
      negative number with the largest absolute value).
  */
  def pick_col : Option[Int] = {
    val l = M(0).data.toList.filter {
      case (i, v) => (num.lt(v, num.zero) && i < n_vars)
    }.sortBy(_._2).map(_._1)

    if(l.isEmpty) None else Some(l.head)
  }

  /** Picks the row whose entry e in column col is > 0 and, among those,
      has the smallest ratio of to row sum.

      Note: As a consequence, for an artificial variable, the value in the
      objective, once non-negative, remains non-negative.
  */
  def pick_row(col: Int) : Option[Int] = {
    var best : Option[(Int, T)] = None
    for(i <- 1 to n_constraints) {
      val r = M(i)
      if(num.gt(r(col), num.zero)) {
        assert(num.gteq(r(n_vars), num.zero))
        val ratio = num.div(r(n_vars), r(col))

        best match {
          case None => { best = Some((i, ratio)) }
          case Some((i0, ratio0)) =>
            if(num.lt(ratio, ratio0)) { best = Some((i, ratio)) }
        }
      }
    }

    best.map(_._1)
  }

  /** use this only for base columns (extactly one nonzero value in column) */
  def get_row(col: Int) : Option[Int] = {
    var result : Option[Int] = None
    for(i <- 1 to n_constraints) if(M(i)(col) != num.zero) result = Some(i)
    result
  }

  /** Runs the simplex algorithm until completion.
      One can also do this manually using (pick_col, pick_row, and) pivot.
  */
  def algo : Option[T] = {
    var it_cnt = 0 // iteration count
    try {
      var next_col = pick_col
      while(next_col != None) {
        val col = next_col.get
        val row = pick_row(col).get
        // throws an exception if there's no suitable
        // row. in that case, there is no upper bound.

        assert(num.gt(M(row)(col), num.zero))
  
        //println("Pivoting at col " + col + " / row " + row)
        M.pivot(row, col)

        base_v(row - 1) = col  // basis variable swap

        // M(0)(n_vars) may be negative, the other M(.)(n_vars) mustn't.
        assert((1 to M.n_rows - 1).forall(
          i => num.gteq(M(i)(n_vars), num.zero)))

        it_cnt += 1
        next_col = pick_col
      }
      //print(it_cnt + " ") // number of pivot operations executed.

      /*
      println("Optimal value: " + M(0)(n_vars))

      for(i <- 0 to n_constraints - 1)
        println("v" + base_v(i) + " = " + M(i + 1)(n_vars))
      */

      Some(M(0)(n_vars))
    }
    catch { case e: Exception => None }
  }

  /** for display */
  def tableau = {
    import breeze.linalg._
    val ar = List("") ++ (0 to n_vars - 1).map(_.toString) ++ List("#")
    val M1 = DenseVector(ar.toArray).toDenseMatrix
    val bs2 = ("Z" :: base_v.toList.map(_.toString)).toArray
    val M2 = DenseVector(bs2).toDenseMatrix.t
    val M3 = M.toDenseMatrix.map(_.toString)
    DenseMatrix.vertcat(M1, DenseMatrix.horzcat(M2, M3))
  }
}


