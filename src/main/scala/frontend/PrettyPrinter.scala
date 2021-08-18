//package ch.epfl.data.sudokube
package frontend
import core._
import breeze.linalg._
import schema._


// TODO: let the user choose which dimensions to put where.
object PrettyPrinter {

  def formatPivotTable(
    sch: Schema,
    q: List[Int],
    bou: Seq[Interval[Rational]]
  ) = {

    val n_bits = q.length
    val bH = n_bits / 2
    val bV = n_bits - bH
    val qV = q.take(bV)
    val qH = q.drop(bV)

    val r: Array[String] = bou.map(x => x.format(y => y.toString)).toArray
    val M = new DenseMatrix(1 << bV, 1 << bH, r)

    val top = DenseMatrix.zeros[String](1, M.cols)
    for(i <- 0 to M.cols - 1) top(0, i) = ('a' + i).toChar.toString

    val left = DenseMatrix.zeros[String](M.rows + 1, 1)
    for(i <- 1 to M.rows ) left(i, 0) = ('A' -1 + i).toChar.toString
    left(0, 0) = ""

    // the n_bits/2 least significant bits are on the vertical axis
    println("Vertical: "   + qV)
    println("Horizontal: " + qH)

    println(sch.decode_dim(qV).zipWithIndex.map{
      case(p, i) => (left(i + 1, 0), p)})

    println(sch.decode_dim(qH).zipWithIndex.map{
      case(p, i) => (top(0, i), p)})

    DenseMatrix.horzcat(left, DenseMatrix.vertcat(top, M)).toString
  }

  def printRelTable(sch: Schema, q: List[Int], bou: Seq[Interval[Rational]]) {
    val result = bou.toList.map(_.lb.get)
    val R2 = sch.decode_dim(q).zip(result)
    R2.foreach{ case (l, v) => println((l,v)) }
  }
}


