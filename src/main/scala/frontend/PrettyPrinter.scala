//package ch.epfl.data.sudokube
package frontend
import core._
import breeze.linalg._
import schema._
import util.Bits


// TODO: let the user choose which dimensions to put where.
object PrettyPrinter {

  def formatPivotTable(
    sch: Schema,
    qV: List[Int],
    qH: List[Int])(
    bou: Seq[Interval[Rational]]
  ) = {


    val bH = qH.size
    val bV = qV.size


    val r: Array[String] = bou.map(x => x.format(y => y.toString)).toArray

    val q_unsorted = (qV ++ qH)
    val q_sorted = q_unsorted.sorted
    val perm = q_unsorted.map(b => q_sorted.indexOf(b)).toArray
    val permBack = qV.sorted.map(b => qV.indexOf(b)).toArray
    val permf = Bits.permute_bits(q_unsorted.size, perm)
    val permfBack = Bits.permute_bits(qV.sorted.size, permBack)
    val r2 = new Array[String](r.size)
    r.indices.foreach( i => r2(permf(i)) = r(i))
    //println("permf = " + permf(6))
    //println("R = " + r.mkString(";"))
    //println("R2 = " + r2.mkString(";"))

    val M = new DenseMatrix(1 << bV, 1 << bH, r2)


    val top = DenseMatrix.zeros[String](1, M.cols)
    for(i <- 0 to M.cols - 1) top(0, i) = ('a' + i).toChar.toString
    if (qH.nonEmpty) {
      sch.decode_dim(qH).zipWithIndex.foreach(pair => top(0, pair._2) = pair._1.mkString(","))
    }

    val left: DenseMatrix[String] = DenseMatrix.zeros[String](M.rows + 1, 1)
    for(i <- 1 to M.rows ) left(i, 0) = ('A' -1 + i).toChar.toString
    left(0, 0) = ""
    if (qV.nonEmpty) {
      sch.decode_dim(qV).zipWithIndex.foreach(pair => left(pair._2+1, 0) = pair._1.mkString(","))
    }

    // the n_bits/2 least significant bits are on the vertical axis
    println("Vertical: "   + qV)
    println("Horizontal: " + qH)

    println(sch.decode_dim(qV).zipWithIndex.map{
      case(p, i) => (left(i + 1, 0), p)})


    println(sch.decode_dim(qH).zipWithIndex.map{
      case(p, i) => (top(0, i), p)})



    val d = DenseMatrix.horzcat(left, DenseMatrix.vertcat(top, M))

    exchangeRows(d, permfBack).toString()

  }

  def exchangeRows(matrix : DenseMatrix[String], permfBack: BigInt => Int): DenseMatrix[String] = {
    val temp = matrix.copy
    for (i <- 0 until matrix.rows-1) {
      for (j <- 0 until matrix.cols) {
        if (matrix.valueAt(i, j) != null) {
          temp.update(i, j, matrix.valueAt(permfBack(i), j))
        }
      }
    }
    temp
  }

  def printRelTable(sch: Schema, q: List[Int], bou: Seq[Interval[Rational]]) {
    val result = bou.toList.map(_.lb.get)
    val R2 = sch.decode_dim(q).zip(result)
    R2.foreach{ case (l, v) => println((l,v)) }
  }
}


