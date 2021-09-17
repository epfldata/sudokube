package core.solver

import core._
import util.Profiler
import util.Util.mkAB

import scala.util.control.Breaks._

class DualSimplex[T](
                      nv: Int, // all vars, no artificial vars
                      _base_v: Seq[Int],
                      constraints: Seq[SparseRow[T]]
                    )(implicit num: Fractional[T]) {
  var n_vars = nv
  var sliceFunc = (x: Int) => true
  var debug = false
  var isOptimal = false
  var terminate = false
  var isFeasible = false



  //val solution = collection.mutable.Map[Int, T]().withDefaultValue(num.zero)

  //def eval(i: Int): Option[T] = {
  //  M.data(i).map(_.evaluate(solution))
  //}

  //var freeRows = List[Int]()
  //var totalFreed = 0
  val base_v = collection.mutable.ArrayBuffer[Int]()
  base_v ++= _base_v

  def n_constraints = base_v.size

  var minVals: collection.mutable.Map[Int, T] = null
  //minVals ++= (0 until nv).map(_ -> num.zero)
  var maxVals: collection.mutable.Map[Int, T] = null//store upper bounds for artificial constraint

  //0th row for objective function and 0th column for row constant.
  val tableau = collection.mutable.ArrayBuffer[SparseRow[T]]()
  tableau += null
  tableau ++= constraints


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

  ///**
  // * Assumes equality constraints.
  // * Assumes all coeffs in l are 1
  // * Assumes b is positive
  // */
  //def addConstraint(l: List[(Int, T)], b: T) = {
  //
  //  //for each vars, check if we have better upper bound
  //  assert(num.gteq(b, num.zero))
  //  l.foreach { case (k, c) =>
  //    assert(c == num.one)
  //    if (!maxVals.isDefinedAt(k) || num.lt(b, maxVals(k)))
  //      maxVals(k) = b
  //  }
  //
  //  val l1 = l.map { case (k, v) => k + 1 -> v }
  //  val row = if (true) {
  //    M.data += mk_constraint(l1, b)
  //    base_v += -100 //DUMMY
  //    n_constraints
  //  } else {
  //    val fR = freeRows.head
  //    freeRows = freeRows.tail
  //    M.data(fR) = mk_constraint(l1, b)
  //    base_v(fR - 1) = -100
  //    fR
  //  }
  //
  //  fixRow(row)
  //  val keyDomain = M(row).data.filterKeys(_ != 0).map(_._1) //choose any non-zero coefficent key column from the new row.
  //  val sliceKeyDomain = (keyDomain.filter(k => sliceFunc(k - 1)))
  //  val bv = if (sliceKeyDomain.isEmpty)
  //    keyDomain.head
  //  else sliceKeyDomain.head
  //
  //  base_v(row - 1) = bv - 1
  //  pivot(row, bv)
  //  isFeasible = false
  //}

  ////Assumes l has indexes > 0
  //private def mk_constraint(l: List[(Int, T)], b: T) = {
  //  //assert(num.gteq(b, num.zero))
  //  Some(SparseRow[T](n_vars + 1, ((0, b) :: l).toMap))
  //}

  /**
   *  Rewrite  a row  in terms of non-basic variables
   *  @param to_row index of row
   */
  def fixRow(to_row: Int) = {
    for ((i, r) <- base_v.zipWithIndex) {
      val from_row = r + 1
      if (tableau(to_row)(i + 1) != num.zero && to_row != from_row) {
        axpy(to_row, from_row, i + 1)
      }
    }
  }


  /** use this only for base columns (exactly one nonzero value in column) */
  def get_row(col: Int): Option[Int] = {
    var result: Option[Int] = None
    for (i <- 1 to n_constraints) if (tableau(i)(col) != num.zero) result = Some(i)
    result
  }

  def run(v: Int, maximize: Boolean): Unit = {
    set_simple_objective(v, maximize)
    it_cnt = 0
    terminate = false
    dual_algo
    val m00 = tableau(0)(0)
    if (maximize) {
      val opt = num.negate(m00)
      if (num.lt(opt, maxVals(v))) {
        maxVals(v) = opt
      }
    } else {
      val opt = m00
      if (num.gt(opt, minVals(v))) {
        minVals(v) = opt
      }
    }
  //undoArtitifical()
  }

  /**
   * Adds objective row to matrix
   * Note: we are minimizing the objective function in the algorithm
   */
  def set_simple_objective(v: Int, maximize: Boolean): Unit = {
    val l = if (maximize) {
      Map(v + 1 -> num.one)
    } else {
      Map(v + 1 -> num.negate(num.one))
    }
    tableau(0) = SparseRow[T](n_vars + 1, l)
    if (maximize) isOptimal = false
    Profiler.noprofile("Fix Row") { fixRow(0)}
    Profiler.noprofile("Make Dual Feasible"){makeDualFeasible()}

  }


  //def undoArtitifical() = {
  //  if(art_row_id != -1)
  //  {
  //    art_row_id = (1 to n_constraints).find(i => M.data(i) != None && M(i).data.isDefinedAt(art_col_id)).get
  //    assert(M(art_row_id).data.isDefinedAt(art_col_id))
  //    pivot(art_row_id, art_col_id)
  //    art_col_id = -1
  //    art_row_id = -1
  //  }
  //}
  /**
   * Makes the current objective function dual feasible
   * Replaces variables with positive coefficient by introducing artificial constraint
   * [[https://www.iro.umontreal.ca/~ferland/ift6575/remarques/Init_sol_Dual_Simplex_Decision%20Science%207%20(1976)_165.pdf]]
   */
  def makeDualFeasible(): Unit = {
    if (!isOptimal) {
      //assert(art_col_id == -1)
      //assert(art_row_id == -1)

      val keyCoeffs = tableau(0).data.filterKeys(_ != 0)
      val posCoeffs = keyCoeffs.filter { case (j, coeff) => num.gt(coeff, num.zero) }
      //val negCoeffs = keyCoeffs.filter { case (j, coeff) =>  num.lt(coeff, num.zero) }
      if (posCoeffs.isEmpty)
        isOptimal = true
      else {
        val maxPosCoeff = posCoeffs.foldLeft(posCoeffs.head) {
          case (acc@(_, ca), cur@(j, cc)) => if (num.gt(cc, ca)) cur else if (cc == ca && j > nv) cur else acc
        }
        val bigM2 = keyCoeffs.map {
          case (0, _) => num.zero
          case (j, coeff) if num.lt(coeff, num.zero) => {
            //assert(num.gteq(solution(j), minVals(j-1)))
            num.times(coeff, minVals(j-1))
          }
          case (j, coeff) if num.gt(coeff, num.zero) => {
            //assert(num.lteq(solution(j), maxVals(j-1)))
            num.times(coeff, maxVals(j-1))
          }
        }.sum
        val bigM = num.fromInt(num.toInt(bigM2) + 1)


        //val bigM = posCoeffs.map { case (j, _) => maxVals(j - 1) }.sum
        //
        //val gap2 = keyCoeffs.map {
        //  case (0, _) => num.zero
        //  case (j, coeff) if num.lt(coeff, num.zero) => {
        //    //assert(num.lteq(solution(j), maxVals(j-1)))
        //    num.times(coeff, maxVals(j-1))
        //  }
        //  case (j, coeff) if num.gt(coeff, num.zero) => {
        //    //assert(num.gteq(solution(j), minVals(j-1)))
        //    num.times(coeff, minVals(j-1))
        //  }
        //}.sum

        //val gap = num.fromInt(num.toInt(gap2) - 1)
        //assert(num.lteq(gap, num.zero))
        assert(num.gteq(bigM, num.zero))

        //assert(num.lteq(bigM, num.fromInt(1000)))
        //assert(num.gteq(gap, num.fromInt(-1000)))
        //val conList = posCoeffs.mapValues(_ => num.one).toList
        val conList = keyCoeffs

        val art_var = n_vars
        val art_constraint = SparseRow(n_vars + 1, conList ++ List(0 -> bigM, art_var + 1 -> num.one))

        //val actualValue = num.negate(art_constraint.get.evaluate(solution))
        //assert(num.gteq(actualValue, num.zero))
        //assert(num.gteq(num.minus(bigM, gap) ,actualValue))
        //solution += (art_var + 1 -> actualValue)


        //minVals += (art_var -> num.zero)
        //maxVals += (art_var -> num.minus(bigM, gap))

        n_vars += 1
        val row =  {
          tableau += art_constraint
          base_v += art_var
          n_constraints
        }
        //else {
        //  val fR = freeRows.head
        //  freeRows = freeRows.tail
        //  M.data(fR) = art_constraint
        //  base_v(fR - 1) = art_var
        //  fR
        //}
        //art_col_id = art_var + 1
        //art_row_id = row
        //println("Adding constraint " + M(n_constraints))
        pivot(row, maxPosCoeff._1)
      }
    }
    assert(isOptimal)
  }

  /**
   * Performs single row transformation.  R[i_to] += R[i_from] * (-R[i_to][piv_col]/R[i_from][piv_col])
   * If the row contains objective function, checks whether it is primal-optimal (dual-feasible)
   */
  protected def axpy(i_to: Int, i_from: Int, piv_col: Int) {

      val r_to = tableau(i_to)
      assert(i_to != i_from)
      if (r_to(piv_col) != num.zero) { // something to do
        val r_from = tableau(i_from)
        val factor: T = num.negate(num.div(r_to(piv_col), r_from(piv_col)))
        val newrow = r_to + r_from * factor
        //val newrow = if (removeCol) newrow1.dropCol(piv_col) else newrow1
        tableau(i_to) = newrow

        if (i_to == 0) {
          isOptimal = newrow.data.filterKeys(_ != 0).map(kv => num.lt(kv._2, num.zero)).foldLeft(true)(_ && _)
        } else {
          //assert(newrow1.evaluate(solution) == num.zero)
        }

      }
    assert(tableau(i_to)(piv_col) == num.zero)
  }

  def pivot(row: Int, col: Int) {
    //val removeRowCol = col > nv
    val piv_row = tableau(row)

    if (piv_row(col) != num.one)
      tableau(row) = piv_row * num.div(num.one, piv_row(col))

    for (i <- 0 to n_constraints) if (row != i) axpy(i, row, col)
    //if (removeRowCol) {
      //tableau.data(row) = None
      //freeRows = row :: freeRows
      //totalFreed += 1
      //art_row_id = -1
      //art_col_id = -1
    //}
    base_v(row - 1) = col - 1 // basis variable swap
  }

  /**
   * For the Dual algorithm,
   * picks the row for which the bi has the smallest value,
   * provided it's negative (i.e., it picks the row that contains the
   * negative number with the largest absolute value).
   */
  def D_pick_row: Option[Int] = {
    (1 to n_constraints).map(r => r -> tableau(r)(0)).filter {
      case (i, v) => (num.lt(v, num.zero)) //filter negative bi
    }.foldLeft[Option[(Int, T)]](None) {
      case (None, cur) => Some(cur)
      case (acc@Some((_, va)), cur@(_, vc)) => if (num.lt(vc, va)) Some(cur) else acc
    }.map(_._1) // find i such that bi is most negative
  }

  /**
   * For the primal algorithm,
   * picks the column for which the objective has the smallest value,
   * provided it's positive.
   */
  def P_pick_col: Option[Int] = {
    tableau(0).data.filter {
      case (i, v) => (i != 0 && num.gt(v, num.zero)) //filter positive cj
    }.foldLeft[Option[(Int, T)]](None) {
      case (None, cur) => Some(cur)
      case (acc@Some((_, va)), cur@(_, vc)) => if (num.gt(vc, va)) Some(cur) else acc
    }.map(_._1) // find j such that cj is most positive
  }

  /**
   * For the Dual algorithm,
   * Picks the col j whose entry a_rj in row r is < 0 and, among those,
   * has the smallest ratio c_j/a_rj.
   * For the ones with same ratio choose a_rj with most negative value
   * No idea why the choosing most negative a_rj helps
   */
  def D_pick_col(r: Int): Option[Int] = {
    val obj = tableau(0)
    val row = tableau(r)
    val res = row.data.
      filter { case (j, a_rj) => j > 0 && num.lt(a_rj, num.zero) }. //filter negative a_rj
      map { case (j, a_rj) => (j, a_rj, num.div(obj(j), a_rj)) } // compute ratio c_j/a_rj (both cj and a_rj are negative)

    assert(!res.isEmpty)

    val maxR = res.foldLeft[Option[(Int, T, T)]](None) {
      case (None, cur) => Some(cur)
      case (acc@Some((_, _, accratio)), cur@(_, _, curratio)) =>
        if (num.gt(curratio, accratio))
          Some(cur)
        else
          acc
    }

    val minR = res.foldLeft[Option[(Int, T, T)]](None) {
      case (None, cur) => Some(cur)
      case (acc@Some((_, aa, accratio)), cur@(_, ca, curratio)) =>
        if (num.lt(curratio, accratio)) //choose smaller ratio
          Some(cur)
        else if ((curratio == accratio) && num.lt(ca, aa)) //same ratio, choose smaller a value (most negative)
          Some(cur)
        else
          acc
    }

    if (maxR.get._3 == num.zero || (minR.get._3 == num.zero && it_cnt >= iter_limit)) {
      terminate = true
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
      val row = tableau(i)
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
  var iter_limit = 1000

  def dual_algo {

    //debug = true
    var next_row = D_pick_row
    //if (debug) (0 to n_constraints).foreach { printRow}
    breakable {
      while (next_row != None && !terminate && it_cnt < iter_limit) {

        val row = next_row.get
        val col = Profiler.noprofile("PickCol"){D_pick_col(row).get}

        if (terminate)
          break
        // throws an exception if there's no suitable
        // col. in that case, there is no feasible solution
        //if(debug) printRow(0)
        //if(debug) println(M(row).data.filterKeys(k => k == 0 || k == col))
        //if (debug) println("Pivoting at col " + col + " / row " + row)

        Profiler.noprofile("Pivot"){pivot(row, col)}

        it_cnt += 1

        next_row = Profiler.noprofile("PickRow"){D_pick_row}
        //if (debug) (0 to n_constraints).foreach { printRow }
        //debug = it_cnt > 50
        //if (debug) {
        //  print(s">$it_cnt::" + M(0)(0) + "  ")
        //  debug = false
        //}
      }
    }
    //print(it_cnt + "  ")

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
      Some(tableau(0)(0))
    }
    catch {
      case e: Exception => None
    }
  }
}
