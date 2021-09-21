//package ch.epfl.data.sudokube
package core
import breeze.linalg.DenseMatrix


/// For use in SparseMatrix
case class SparseRow[T](n: Int, data: Map[Int, T]
)(implicit num: Numeric[T]) {

  //evaluate row treated as expression given values for varuiables
  def evaluate(values: Int => T) = {
    data.map { case (i, coef) => num.times(coef, values(i))}.sum
  }

  /// fetch i-th item in row
  def apply(i: Int) : T = {
    //assert(i < n)
    data.getOrElse(i, num.zero)
  }

  /// multiplies each value in the row with x.
  def *(x: T) = SparseRow[T](n, data.mapValues(v => num.times(x, v)))

  /// sums up two row vectors
  def plus2(that: SparseRow[T]) : SparseRow[T] = {
    //assert(n == that.n)

    // merge
    val l = (data.toList ++ that.data.toList).groupBy(_._1).mapValues(
      x => x.map(_._2).sum
    ).filter{ case (col, v) => (v != num.zero) }

    SparseRow(n, l)
  }

  /** same behavior as plus2.
      This is about 25% faster than plus2, not a big deal
  */
  def +(that: SparseRow[T]) : SparseRow[T] = {
    //assert(n == that.n)
    val l = this.data.filter { case (v, _) => ! that.data.contains(v)  } ++
            that.data.map { case (v, x) => {
              val y = num.plus(apply(v), x)
              if(y == num.zero) None else Some(v -> y)
            }}.flatten
     SparseRow(n, l.toMap)
  }

  def dropCol(x: Int) = SparseRow(n-1, data.filterKeys(_ != x))

  /** nonempty columns, not sorted */
  def domain : List[Int] = data.toList.map(_._1)
  // def domain : Set[Int] = data.keySet
}


/*
  for(i <- 1 to 2000000) {
  val sz = 10
  def mkMap = (0 to sz-1).map(v => (v, scala.util.Random.nextInt(10))).filter{ case (v, x) => x > 3}.toMap

  val r1 = SparseRow(sz, mkMap)
  val r2 = SparseRow(sz, mkMap)

  r1 + r2
  }

  for(i <- 1 to 2000000) {
  val sz = 10
  def mkMap = (0 to sz-1).map(v => (v, scala.util.Random.nextInt(10))).filter{ case (v, x) => x > 3}.toMap

  val r1 = SparseRow(sz, mkMap)
  val r2 = SparseRow(sz, mkMap)

  r1 plus2 r2
  }
*/


case class SparseMatrix[T](n_rows: Int, var n_cols: Int
)(implicit num: Numeric[T]) {

  val data : collection.mutable.ArrayBuffer[Option[SparseRow[T]]] =
    util.Util.mkAB[Option[SparseRow[T]]](n_rows, _ => None)

  /** throws an exception in case the row is missing. */
  def apply(row: Int) : SparseRow[T] = data(row).get

  /// convert to breeze DenseMatrix
  def toDenseMatrix : DenseMatrix[Double] = {
    val active_rows = data.filter(_ != None)
    val M = DenseMatrix.zeros[Double](active_rows.length, n_cols)

    active_rows.zipWithIndex.foreach {
      case (Some(r), row) => r.data.foreach {
        case (col, v) => M(row, col) = num.toDouble(v)
      }
      case (None, _) => { }
    }
    M
  }

  override def toString = toDenseMatrix.toString

  /** projects a matrix to a subset of its columns.
      The order of the elements of cols matters! One can reorder columns.
  */
  def select_cols(cols: Seq[Int]) : SparseMatrix[T] = {
    val new_n_cols = cols.length
    val M = SparseMatrix[T](n_rows, new_n_cols)
    val new_index = cols.map(x => (x, cols.indexWhere(_ == x))).toMap

    data.zipWithIndex.foreach {
      case (Some(r), i) => {
        val r2 = r.data.filter {
          case (key, _) => cols.contains(key)
        }.map {
          case (key, v) => (new_index(key), v)
        }
        M.data(i) = Some(SparseRow[T](new_n_cols, r2))
      }
      case (None, _) => { }
    }
    M
  }
}


/** import SparseMatrixImplicits._ and SparseMatrix[T: Fractional] has methods
    axpy and pivot.
*/
object SparseMatrixImplicits {

  implicit class SparseMatrixFractionalOps[T](
    M: SparseMatrix[T])(implicit num: Fractional[T]) {

    /** row_to -= row_from * (row_to(piv_col) / row_from(piv_col))
    */
    def axpy(i_to: Int, i_from: Int, piv_col: Int) {
      val row_to = M(i_to)

      if(row_to(piv_col) != num.zero) {
        val row_from = M(i_from)
        val factor: T = num.negate(num.div(row_to(piv_col), row_from(piv_col)))
        M.data(i_to) = Some(row_to + row_from * factor)
      }

      // assert(M(i_to)(piv_col) == num.zero)
    }

    /** The method was moved here from SimplexAlgo and isn't perfectly named.
        Makes a pivot by applying axpy of row with all other rows.
    */
    def pivot(row: Int, col: Int) {
      val piv_row = M(row)
      assert(piv_row(col) != num.zero)

      if(piv_row(col) != num.one)
        M.data(row) = Some(piv_row * num.div(num.one, piv_row(col)))

      for(i <- 0 to (M.n_rows - 1))
        if(row != i)
          M.axpy(i, row, col)
    }
  }
}


object SparseMatrixTools {
  /// converts a breeze DenseMatrix to a SparseMatrix
  def fromDenseMatrix[T: Fractional](M: DenseMatrix[T]) : SparseMatrix[T] = {
    val sparse_M = SparseMatrix[T](M.rows, M.cols)
    for(row <- 0 to M.rows - 1) {
      sparse_M.data(row) = Some(SparseRow[T](M.cols,
        (for(col <- 0 to M.cols - 1 if (M(row, col) != 0)) yield
          (col, M(row, col))
        ).toMap))
    }
    sparse_M
  }
}


