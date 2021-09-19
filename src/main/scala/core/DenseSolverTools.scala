//package ch.epfl.data.sudokube
package core
import util._


object DenseSolverTools {
  import breeze.linalg._

  /** Gaussian elimination with pivots for each of the columns cols (but
      not other columns). Modifies M.
  */
  def gauss_oncols(M: DenseMatrix[Double], cols: Seq[Int]) {
    val cols_ordered = cols.sorted.reverse // from right

    var row = M.rows - 1
    for(col <- cols_ordered) {
      val row0 = M(::, col).toArray.indexWhere(_ != 0)
      assert(row0 >= 0)

      // row swap
      for(i <- 0 to M.cols - 1) {
        val tmp = M(row0, i)
        M(row0, i) = M(row, i)
        M(row, i) = tmp
      }

      M(row,::) :/= M(row, col)
      for(r <- 0 to M.rows - 1) if(r != row) {
        val pv = M(r, col) / M(row, col) 
        M(r, ::) :-= M(row,::) * pv
      }

      row = row - 1
    }
  }

  /** removes
      * linearly dependent rows (after Gaussian elimination has zeroed them) and
      * cols that belong to vars that have been solved.
      A variable is "solved" if it is not among the free vars and
      its row contains only one nonzero value, not counting the rightmost
      column.

      @param M         a matrix that is the result of Gaussian elimination.
      @param free_vars if one removes the columns free_vars and the rightmost
                       column from M, one obtains the identity matrix in 
                       M.cols - freevars.length - 1 dimensions.
      Example:
      {{{
      scala> free_vars
      res0: Seq[Int] = Vector(0, 2)

      scala> M
      res1: breeze.linalg.DenseMatrix[s.NT] =
      1.0   1.0  0.0   0.0  0.0  0.0  0.0  0.0  2.0
      0.0   0.0  1.0   1.0  0.0  0.0  0.0  0.0  2.0
      0.0   0.0  0.0   0.0  1.0  0.0  0.0  0.0  2.0 <-
      -1.0  0.0  0.0   0.0  0.0  1.0  0.0  0.0  0.0
      0.0   0.0  1.0   0.0  0.0  0.0  1.0  0.0  2.0
      0.0   0.0  -1.0  0.0  0.0  0.0  0.0  1.0  0.0

      scala> DenseSolverTools.removeSolved(M, free_vars)
      res2: (Seq[Int], breeze.linalg.DenseMatrix[Double]) =
      (Vector(0, 1, 2, 3, 5, 6, 7),
      1.0   1.0  0.0   0.0  0.0  0.0  0.0  2.0
      0.0   0.0  1.0   1.0  0.0  0.0  0.0  2.0
      -1.0  0.0  0.0   0.0  1.0  0.0  0.0  0.0
      0.0   0.0  1.0   0.0  0.0  1.0  0.0  2.0
      0.0   0.0  -1.0  0.0  0.0  0.0  1.0  0.0  )
      }}}
  */
  def removeSolved(M: DenseMatrix[Double], free_vars: Seq[Int]) = {
    val n_vars = M.cols - 1
    val df = free_vars.length

    val M2  = M(::, free_vars).toDenseMatrix
    var solved_rows = List[Int]()
    var solved_vars = List[Int]()
    for(row <- 0 to M2.rows - 1)
      if(M2(row, ::).t.forall(_ == 0)) {
        solved_rows = row::solved_rows
        val v = M(row, ::).t.toArray.zip(0 to n_vars - 1).indexWhere(x =>
          (x._1 != 0) && (! free_vars.contains(x._2)))
        solved_vars = v :: solved_vars
      }
    val unsolved_rows = Util.complement(0 to n_vars - df - 1, solved_rows)
    val unsolved_vars = Util.complement(0 to n_vars - 1,      solved_vars)

    val M30 = M(unsolved_rows, ::).toDenseMatrix
    val M3 = M30(::, unsolved_vars ++ List(n_vars)).toDenseMatrix
    (unsolved_vars, M3)
  }

  /** returns pairs (row, col) where col is a saved variable and row is its row.
  */
  def solved(M: DenseMatrix[Double]) : Seq[(Int, Int)] = {
    val n_vars = M.cols - 1

    (for(row <- 0 to M.rows - 1) yield {
      val a = M(row, ::).t.toArray.zipWithIndex.filter {
        case (v, _) => (v != 0) }

      a match {
        case Array((1.0, i), (_, n_vars))             => Some((row, i))
        case Array((1.0, i)) if (M(row, n_vars) == 0) => Some((row, i))
        case _ => None
      }
    }).flatten
  }

  /** Simplex algorithm with constraints from <M>. The objectives are,
      individually, maximizing and minimizing each of the <objectives>.
      There are two modes; <with_slack> adds slack variables, so the rows of
      <M> are assumed to be <= inequalities. When <with_slack> is false,
      we assume that the slack variables are in <M>, and so the rows of <M>
      are interpreted at equations.

      This uses the Apache Solver, which has numerical stability issues,
      causing it to claim some scenarios infeasible that aren't, specifically
      when the smallest and largest feasible values for a variable coincide
      (which is important since that's a solved variabel, which we are aiming
      for).

      @param objectives is a collection of objectives. An objective is
             a polynomial, which is represented as a list of
             (coefficient, variable id pairs).
      @bounds contain upper and lower bounds on the variables, which are
              turned into constraints. The i-th bound is for the i-th
              variable which corresponds to the i-th column in <M>.
  */
  def simplex[T](M: SparseMatrix[T],
              bounds: Seq[Interval[T]],
              objectives: Seq[List[(T, Int)]],
              with_slack : Boolean = false
  )(implicit num: Numeric[T]) : Seq[Interval[T]] = {
    val n_vars = M.n_cols - 1
    assert(bounds.length == n_vars)
    import breeze.optimize.linear._

    val lp = new LinearProgram()
    import lp._
    val vars = (1 to n_vars).map(_ => Real())

    // sums up a list of expressions
    def esum(l: List[Expression]) : Expression = {
      def plus(x: Expression, y: Expression) = x + y
      assert(l.length > 0)
      if(l.length == 1) l.head
      else l.tail.foldLeft(l.head)(plus)
    }

    // returns a polynomial of type Expression
    def mk_poly(l: List[(Double, Int)]) = esum(l.map(x => vars(x._2) *  x._1))

    val constraints : Seq[Constraint] =
    (for(row <- 0 to M.n_rows - 1 if (M.data(row) != None)) yield {
      val e0 = M(row).data.toList.filter(_._1 != M.n_cols - 1).map(
        x => (num.toDouble(x._2), x._1))
      // this can be empty if we project away the slack variables

      if(e0.length == 0) None
      else {
        val b = num.toDouble(M(row)(M.n_cols - 1))
        if(with_slack) Some(mk_poly(e0) <=  b)
        else           Some(mk_poly(e0) =:= b)
      }
    }).flatten ++
    vars.zip(bounds).map{
      case(v, Interval(Some(lb), Some(ub))) => List((v >= num.toDouble(lb)),
                                                    (v <= num.toDouble(ub)))
      case(v, Interval(Some(lb), None    )) => List(v >= num.toDouble(lb))
      //case(v, Interval(Some(lb), _       )) => List(v >= 0.0)
      case(v, Interval(None,     Some(ub))) => List(v <= num.toDouble(ub))
      case(v, Interval(None,     None    )) => List[Constraint]()
      //case(v, Interval(None,     _       )) => List[Constraint]()
    }.flatten

    objectives.map{ o => {
      val o2 = o.map(x => (num.toDouble(x._1), x._2))
      val problem = mk_poly(o2).subjectTo(constraints: _*)

      // TODO: support multi-variable objectives
      val i = o(0)._2 // index of variable for which we have optimized

      // TODO: catch undetermined cases
      print("@")
      val lb = num.fromInt(minimize(problem).result(i).round.toInt)
      print("$")
      val ub = num.fromInt(maximize(problem).result(i).round.toInt)
      if(num.gt(lb, ub)) println("Apache is utter crap")

      Interval(Some(lb), Some(ub))
    }}
  }

  def mk_all_non_neg[T](n_bits: Int)(implicit num: Numeric[T]
  ): collection.mutable.ArrayBuffer[Interval[T]] = {

    val bounds = new collection.mutable.ArrayBuffer[Interval[T]]()
    bounds ++= (1 to n_bits).map(_ => Interval(Some(num.zero), None))
    bounds
  }
} // end DenseSolverTools


