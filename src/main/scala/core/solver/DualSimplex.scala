package core.solver

import core.{Rational, SparseMatrix, SparseRow}
import util.Util.mkAB

class DualSimplex[T](
                      nv: Int, // all vars
                      _base_v: Seq[Int],
                      constraints: List[(List[(Int, T)], T)]
                    )(implicit num: Fractional[T]) {
  var n_vars = nv

  var debug = false
  var isOptimal = false
  var terminate = false
  var isFeasible = false

  val base_v = collection.mutable.ArrayBuffer[Int]()
  val parallelObj = collection.mutable.Map[(Int, Boolean), T]()
  def n_constraints = base_v.size

  val maxVals = collection.mutable.Map[Int, T]()  //store upper bounds for artificial constraint

  //0th row for objective function and 0th column for row constant.
  val M = SparseMatrix[T](n_constraints + 1, n_vars + 1)
  base_v ++= _base_v
  M.data ++= constraints.map { case (l, b) => mk_constraint(l.map { case (k, v) => k + 1 -> v }, b) }


  //def checkIsParallelObj(r: Int) : Option[((Int, Boolean), T)]= {
  //  assert(r > 0)
  //  val row = M(r)
  //  val basisVar = base_v(r-1)
  //  val signs = row.data.filterKeys(k => k != 0 && k != basisVar + 1 ).mapValues(v => num.gt(v, num.zero)).map(_._2)
  //  val allPos = signs.reduce(_ && _)
  //  val allNeg = !signs.reduce(_ || _)
  //  if(!allPos && !allNeg)
  //    None
  //  else {
  //    val isMax = allPos
  //  }
  //}

  //Assumes l has indexes > 0
  private def mk_constraint(l: List[(Int, T)], b: T) = {
    //assert(num.gteq(b, num.zero))
    Some(SparseRow[T](n_vars + 1, ((0, b) :: l).toMap))
  }

  /**
   *  Rewrite  a row  in terms of non-basic variables
   *  @param to_row index of row
   */
  def fixRow(to_row: Int) = {
    for ((i, r) <- base_v.zipWithIndex) {
      val from_row = r + 1
      if (M(to_row)(i + 1) != num.zero && to_row != from_row) {
        axpy(to_row, from_row, i + 1)
      }
    }
  }


  /** use this only for base columns (exactly one nonzero value in column) */
  def get_row(col: Int): Option[Int] = {
    var result: Option[Int] = None
    for (i <- 1 to n_constraints) if (M(i)(col) != num.zero) result = Some(i)
    result
  }

  /**
   * Adds objective row to matrix
   * Note: we are minimizing the objective function in the algorithm
   */
  def set_simple_objective(v: Int, maximize: Boolean): Unit = {
    val l = if (maximize) {
      List(v + 1 -> num.one)
    } else {
      List(v + 1 -> num.negate(num.one))
    }
    M.data(0) = Some(SparseRow[T](n_vars + 1, l.toMap))
    fixRow(0)
    makeDualFeasible()
  }

  /**
   * Makes the current objective function dual feasible
   * Replaces variables with positive coefficient by introducing artificial constraint
   * [[https://www.iro.umontreal.ca/~ferland/ift6575/remarques/Init_sol_Dual_Simplex_Decision%20Science%207%20(1976)_165.pdf]]
   */
  def makeDualFeasible(): Unit = {
    if (!isOptimal) {
      val posCoeffs = M(0).data.filter { case (j, coeff) => (j != 0) && num.gt(coeff, num.zero) }
      if (posCoeffs.isEmpty)
        isOptimal = true
      else {
        val maxPosCoeff = posCoeffs.foldLeft(posCoeffs.head) {
          case (acc@(_, ca), cur@(_, cc)) => if (num.gt(cc, ca)) cur else acc
        }
        val bigM = posCoeffs.map { case (j, _) => maxVals(j - 1) }.sum
        n_vars += 1
        M.data += mk_constraint((n_vars -> num.one) :: posCoeffs.mapValues(_ => num.one).toList, bigM)
        base_v += maxPosCoeff._1 - 1
        //println("Adding constraint " + M(n_constraints))
        pivot(n_constraints, maxPosCoeff._1)
      }
    }
    assert(isOptimal)
  }

  /**
   * Performs single row transformation.  R[i_to] += R[i_from] * (-R[i_to][piv_col]/R[i_from][piv_col])
   * If the row contains objective function, checks whether it is primal-optimal (dual-feasible)
   */
  protected def axpy(i_to: Int, i_from: Int, piv_col: Int) = {
    val r_to = M(i_to)
    assert(i_to != i_from)
    if (r_to(piv_col) != num.zero) { // something to do
      val r_from = M(i_from)
      val factor: T = num.negate(num.div(r_to(piv_col), r_from(piv_col)))
      val newrow = r_to + r_from * factor
      M.data(i_to) = Some(newrow)
      if (i_to == 0) {
        isOptimal = newrow.data.filterKeys(_ != 0).map(kv => num.lt(kv._2, num.zero)).foldLeft(true)(_ && _)
      }
      assert(M(i_to)(piv_col) == num.zero)
      newrow
    } else
      r_to
  }

  def pivot(row: Int, col: Int) {
    val piv_row = M(row)

    if (piv_row(col) != num.one)
      M.data(row) = Some(piv_row * num.div(num.one, piv_row(col)))

    for (i <- 0 to n_constraints) if (row != i) axpy(i, row, col)

    base_v(row - 1) = col - 1 // basis variable swap
  }

  /**
   * For the Dual algorithm,
   * picks the row for which the bi has the smallest value,
   * provided it's negative (i.e., it picks the row that contains the
   * negative number with the largest absolute value).
   */
  def D_pick_row: Option[Int] = {
    (1 to n_constraints).map(r => r -> M(r)(0)).filter {
      case (i, v) => (num.lt(v, num.zero)) //filter negative bi
    }.foldLeft[Option[(Int, T)]](None) {
      case (None, cur) => Some(cur)
      case (acc@Some((_, va)), cur@(_, vc)) => if (num.lt(vc, va)) Some(cur) else acc
    }.map(_._1) // find i such that bi is most negative
  }

  /**
   * For the primal algorithm,
   *  picks the column for which the objective has the smallest value,
      provided it's positive.
   */
  def P_pick_col: Option[Int] = {
    M(0).data.filter {
      case (i, v) => (i != 0 && num.gt(v, num.zero)) //filter positive cj
    }.foldLeft[Option[(Int, T)]](None) {
      case (None, cur) => Some(cur)
      case (acc@Some((_, va)), cur@(_, vc)) => if (num.gt(vc, va)) Some(cur) else acc
    }.map(_._1) // find j such that cj is most positive
  }

  /**
   * For the Dual algorithm,
   *  Picks the col j whose entry a_rj in row r is < 0 and, among those,
   * has the smallest ratio c_j/a_rj.
   * For the ones with same ratio choose a_rj with most negative value
   * No idea why the choosing most negative a_rj helps
   */
  def D_pick_col(r: Int): Option[Int] = {
    val obj = M(0)
    val row = M(r)
    val res = row.data.
      filter { case (j, a_rj) => j > 0 && num.lt(a_rj, num.zero) }. //filter negative a_rj
      map { case (j, a_rj) => (j, a_rj, num.div(obj(j), a_rj)) } // compute ratio c_j/a_rj (both cj and a_rj are negative)

    val maxR = res.foldLeft[Option[(Int, T, T)]](None) {
      case (None, cur) => Some(cur)
      case (acc@Some((_, _, accratio)), cur@(_, _, curratio)) =>
        if (num.gt(curratio, accratio))
          Some(cur)
        else
          acc
    }

    if (maxR.get._3 == num.zero) {
      terminate = true
    }

    val minR = res.foldLeft[Option[(Int, T, T)]](None) {
      case (None, cur) => Some(cur)
      case (acc@Some((_, aa, accratio)), cur@(_, ca, curratio)) =>
        if (num.lt(curratio, accratio))  //choose smaller ratio
          Some(cur)
        else if((curratio == accratio) && num.lt(ca, aa))  //same ratio, choose smaller a value (most negative)
          Some(cur)
        else
          acc
    }

    //if(debug) println("max = "+maxR+ " min = "+minR)
    minR.map(_._1)
  } //find j that minimizes c_j/a_rj


  /**
   * For the primal algorithm
   * Picks the col j whose entry a_rj in row r is < 0 and, among those,
   * has the smallest ratio c_j/a_rj
   */
  def P_pick_row(c: Int): Option[Int] = {
    (1 to n_constraints).map { i =>
      val row = M(i)
      val b = row(0)
      val a_ic = row(c)
      (i, b, a_ic)
    }.
      filter { case (i, b, a_ic) => num.gt(a_ic, num.zero) }. //filter positive a_ic
      map { case (i, b, a_ic) => i -> num.div(b, a_ic) }. // compute ratio b_i/a_ic (both b_i and a_ic are positive)
      foldLeft[Option[(Int, T)]](None) {
        case (None, cur) => Some(cur)
        case (acc@Some((_, accratio)), cur@(_, curratio)) => if (num.lt(curratio, accratio)) Some(cur) else acc
      }.map(_._1) //find i that minimizes b_i/a_ic
  }


  //def printRow(i : Int) = println(s"DUAL $i :: basis = ${if (i > 0) base_v(i - 1) else "N/A"} -> " + M(i).evaluate(sol) + " <== " + M(i))

  var it_cnt = 0 // iteration count

  def dual_algo: Option[T] = {

    //debug = true
    var next_row = D_pick_row
    //if (debug) (0 to n_constraints).foreach { printRow}
    while (next_row != None && !terminate) {

      val row = next_row.get
      val col = D_pick_col(row).get
      // throws an exception if there's no suitable
      // col. in that case, there is no feasible solution
      //if(debug) printRow(0)
      //if(debug) println(M(row).data.filterKeys(k => k == 0 || k == col))
      //if (debug) println("Pivoting at col " + col + " / row " + row)
      pivot(row, col)

      it_cnt += 1
      next_row = D_pick_row

      //if (debug) (0 to n_constraints).foreach { printRow }
      //debug = it_cnt > 50
      //if (debug) {
      //  print(s">$it_cnt::" + M(0)(0) + "  ")
      //  debug = false
      //}
    }
    //print(it_cnt + "  ")
    isFeasible = true
    Some(M(0)(0))

  }

  def primal_algo: Option[T] = {
    var it_cnt = 0 // iteration count
    try {
      var next_col = P_pick_col
      while (next_col != None) {
        //(0 to n_constraints).foreach{i => println(s"PRIMAL $i ==> " +M(i)+ " == " + M(i).evaluate(sol))}
        val col = next_col.get
        val row = P_pick_row(col).get
        // throws an exception if there's no suitable
        // row. in that case, there is no feasible solution

        //println("Pivoting at col " + col + " / row " + row)
        pivot(row, col)

        it_cnt += 1
        next_col = P_pick_col
      }
      //print(it_cnt + "  ")
      isOptimal = true
      Some(M(0)(0))
    }
    catch {
      case e: Exception => None
    }
  }
}
