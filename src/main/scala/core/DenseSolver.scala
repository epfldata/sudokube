//package ch.epfl.data.sudokube
package core
import util._
import backend.Payload


/** The main solver; uses Gaussian elimination and linear programming.
    These two algos can be found in DenseSolverTools.

    @param n_bits       the length of the query. The query consists of
                        the bits 0 ... n_bits - 1.
    @param projections  subsets of q
    @param v0           is the values read from the cuboids for these
                        projections, in the same order as in the list
                        projections.
*/
case class DenseSolver(
  n_bits: Int,
  bounds: collection.mutable.ArrayBuffer[Interval[Double]],
  projections: List[List[Int]],
  v: Array[Payload]
) {
  assert(n_bits < 31)
  val n_vars = 1 << n_bits
  assert(bounds.length == n_vars)

  import breeze.linalg._
  var M = DenseMatrix.zeros[Double](0, n_vars + 1)
  private val det_vars = collection.mutable.Set[Int]()
  def free_vars : Seq[Int] = Util.complement((0 to n_vars - 1), det_vars.toSeq)
  def df : Int = free_vars.length // # degrees of freedom

  /** We can add further equations whenever we like. */
  def add(eqs: Seq[(Seq[Int], Payload)]) {

    print("Adding " + eqs.length + " rows ... ")

    var rid = 0
    val eqs_filtered =
      (for(eq <- eqs) yield {
        // the bound from the payload applies for all variables in the eq.
/*
        if(eq._2.i != None) for(x <- eq._1) {
          bounds(x) = bounds(x).intersect(eq._2.i.get * (1 << (60 - n_bits)))
        }
*/
        if (!det_vars.contains(eq._1.last)) {
          det_vars.add(eq._1.last)
          val t = (rid, eq._1, eq._2.sm)
          rid += 1
          Some(t)
        }
        else None
      }).flatten

    val M2 = DenseMatrix.zeros[Double](eqs_filtered.length, n_vars + 1)
    eqs_filtered.foreach { case (row, vars, total) => 
      for(pos <- vars) {
        M2(row, pos) = 1

        // set naive upper bound.
        // These are not correct unless the lower bounds are all zero.
        bounds(pos) = bounds(pos).intersect(Interval(None, Some(total)))
      }
      M2(row, n_vars) = total
    }
    M = DenseMatrix.vertcat(M2, M)

    println("done (" + eqs_filtered.length + ")")
  }

  def gauss() { DenseSolverTools.gauss_oncols(M, det_vars.toSeq) }

  val eqs_sparse: List[(Seq[Int], Payload)] =
    SolverTools.mk_constraints(n_bits, projections, v).toList


  add(eqs_sparse)
  // gauss()


  /** solve for all free vars and put into constraint format, to be added
      as new rows to the matrix. Also refines bounds.
      Must do Gaussian Elimination first, we are removing the determined
      variables here.
  */
  def simplex_add() = {

    // remove the determined vars. The Simplex implementation adds its own
    // slack vars.
    val M2 = M(::, free_vars ++ List(n_vars)).toDenseMatrix
    val M2_sparse = SparseMatrixTools.fromDenseMatrix(M2)

    val bounds0 = Util.filterIndexes(bounds, free_vars)
    val objectives = (0 to df - 1).map(x => List((1.0, x)))

    val new_bounds: Seq[Interval[Double]] =
      DenseSolverTools.simplex(M2_sparse, bounds0, objectives, true)

    free_vars.zip(new_bounds).foreach { case(v, i) =>
      bounds(v) = bounds(v).intersect(i)
    }

    val point_intervals = free_vars.zip(new_bounds).filter{
        case (_, Interval(Some(lb), Some(ub))) => lb == ub
        case _ => false
      }

    val new_eqs = point_intervals.map(x =>
        (collection.immutable.Vector(x._1), new Payload(x._2.lb.get, None)))

    if(! new_eqs.isEmpty) {
      add(new_eqs)
      gauss // we could run DenseSolverTools.gauss_oncols(M, newly_det_vars)
    }
  }

  /** infers bounds for solved variables. See DenseSolverTools.solved().
  */
  def bound_from_solved() = {
    val x : Seq[(Int, Int)] = DenseSolverTools.solved(M)

    x.foreach {
      case (row, v) => {
        val b = Some(M(row, n_vars))
        bounds(v) = bounds(v).intersect(Interval(b, b))
      }
    }
  }

  /** refines bounds for determined variables.
      This is not just useful after running the simplex algorithm.
      It can infer bounds from the
      initial bounds given to the solver, and those set by add().
  */
  def compute_det_bounds() = {
    gauss() // must be gaussed, otherwise we try to set lower bounds that
            // are greater than the upper bounds.

    // it's because this function will do the wrong thing.
    def findDetVar(row: Int) = {
      (M(row, ::).t.toArray.zipWithIndex.filter { case (v, i) =>
        (v != 0) && (det_vars.contains(i))
      })(0)._2
    }

    //for(v <- free_vars) println("v" + v + " = " + bounds(v))

    for(row <- 0 to M.rows - 1) {
      val det_var = findDetVar(row)
      val a = M(row, ::).t.toArray
      val b : Double = a(n_vars)

      def unforce(x: Double) : Option[Double] = {
        val inf = 1.0/0
        val na = inf - inf
        if((x == inf) || (x == - inf) || (x == na)) None
        else Some(x)
      }

      def force_lb(i: Interval[Double]) = i.lb.getOrElse(-1.0/0)
      def force_ub(i: Interval[Double]) = i.ub.getOrElse( 1.0/0)

      // compute_upper_bound
      val ov_lb : Double = (for(v <- free_vars) yield
        force_lb(bounds(v) * a(v))
      ).sum
      val proposal_ub = unforce(b - ov_lb)

      // compute_lower_bound
      val ov_ub : Double = (for(v <- free_vars) yield
        force_ub(bounds(v) * a(v))
      ).sum
      val proposal_lb = unforce(b - ov_ub)

      val proposal = Interval(proposal_lb, proposal_ub)

      /*
      println("[row " + row + "]  v" + det_var
        + ": " + b + " - [" + ov_lb + ", " + ov_ub + "] = "
        + proposal + " vs old = "
        + bounds(det_var))
      */

      bounds(det_var) = bounds(det_var).intersect(
        Interval(proposal_lb, proposal_ub))
    }
  }


  def compute_bounds() : Array[Interval[Double]] = {

    println("cb 1")
    gauss // necessary
    println("cb 2")
    bound_from_solved
    println("cb 3")

    if(df != 0) {
    if(df < 25) {

    try { simplex_add }
    catch { case e: Exception => println("Apache is crap.") }

    println("cdb BEGIN")
    compute_det_bounds
    println("cdb END")

    if((df != 0) && (det_vars.size <= 20)) {
      val det_vars_ordered = det_vars.toList
      val objectives = det_vars_ordered.map(x => List((1.0, x)))
      //val objectives = (0 to n_vars - 1).toList.map(x => List((1.0, x)))

      try {
        val M_sparse = SparseMatrixTools.fromDenseMatrix(M)
        val new_bounds =
          DenseSolverTools.simplex(M_sparse, bounds, objectives, false).toArray

        val paired = det_vars_ordered.zip(new_bounds)

        //if(new_bounds.toList != bounds.toList) {
        //  println("IMPROVEMENT: " + paired.toList + " vs " + bounds.toList)
        //}

        paired.foreach { case (v, i) => { bounds(v) = bounds(v).intersect(i) }}
      }
      catch { case e: Exception => println("Apache REALLY is crap.") }
    }

    } else {
      println("#df = " + df + ", skipping optimization.")
      compute_det_bounds
    }
    }

    bounds.toArray
  }
} // end DenseSolver


