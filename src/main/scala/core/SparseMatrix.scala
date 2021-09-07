//package ch.epfl.data.sudokube
package core
import util._
import breeze.linalg._


case class SparseRow[T](n: Int, data: Map[Int, T]
)(implicit num: Numeric[T]) {

  def evaluate(values: Map[Int, T]) = {
    data.map { case (i, coef) => num.times(coef, values.getOrElse(i, num.zero))}.sum
  }
  def apply(i: Int) : T = {
    //assert(i < n)
    data.getOrElse(i, num.zero)
  }

  def *(x: T) = SparseRow[T](n, data.mapValues(v => num.times(x, v)))

  def plus2(that: SparseRow[T]) : SparseRow[T] = {
    //assert(n == that.n)

    // merge
    val l = (data.toList ++ that.data.toList).groupBy(_._1).mapValues(
      x => x.map(_._2).sum
    ).filter{ case (col, v) => (v != num.zero) }

    SparseRow(n, l)
  }

  // this is about 25% faster that plus2, not a big deal
  def +(that: SparseRow[T]) : SparseRow[T] = {
    //assert(n == that.n)
    val l = this.data.filter { case (v, _) => ! that.data.contains(v)  } ++
            that.data.map { case (v, x) => {
              val y = num.plus(apply(v), x)
              if(y == num.zero) None else Some(v -> y)
            }}.flatten
     SparseRow(n, l.toMap)
  }

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

  val data = Util.mkAB[Option[SparseRow[T]]](n_rows, _ => None)

  /** throws an exception in case the row is missing. */
  def apply(row: Int) : SparseRow[T] = data(row).get

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
      The order of the elements of cols matters! One can reorder columns
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


object SparseMatrixTools {
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


