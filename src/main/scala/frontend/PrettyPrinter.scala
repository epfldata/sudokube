//package ch.epfl.data.sudokube
package frontend
import core._
import breeze.linalg._
import core.solver.Rational
import core.solver.lpp.Interval
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
    val permf = Bits.permute_bits(q_unsorted.size, perm)

    val permBackqV= qV.sorted.map(b => qV.indexOf(b)).toArray
    val permfBackqV = Bits.permute_bits(qV.size, permBackqV)
    val permBackqH= qH.sorted.map(b => qH.indexOf(b)).toArray
    val permfBackqH = Bits.permute_bits(qH.size, permBackqH)

    var M = new DenseMatrix[String](1 << bV, 1 << bH)
    for (i<- 0 until M.rows) {
      for (j<- 0 until M.cols) {
        M(i, j) = r(permf(j*M.rows + i))
      }
    }


    val top = DenseMatrix.zeros[String](1, M.cols)
    for(i <- 0 until M.cols) {
      top(0, i) = ('a' + i).toChar.toString

    }
    for (i<- 0 until (1 << bV)) {
      println(permf(i))
      println(permfBackqV(i))
    }
    if (qH.nonEmpty) {
      sch.decode_dim(qH).zipWithIndex.foreach(pair => top(0, permfBackqH(pair._2)) = pair._1.mkString(",").replace(" in List", "="))
    }

    val left: DenseMatrix[String] = DenseMatrix.zeros[String](M.rows + 1, 1)
    for(i <- 1 to M.rows ) {
      left(i, 0) = ('A' -1 + i).toChar.toString
    }
    left(0, 0) = ""
    if (qV.nonEmpty) {
      sch.decode_dim(qV)
        .zipWithIndex.foreach(pair => left(permfBackqV(pair._2) + 1, 0) = pair._1.mkString(",").replace(" in List", "="))
    }


    // the n_bits/2 least significant bits are on the vertical axis
    println("Vertical: "   + qV)
    println("Horizontal: " + qH)


    M = exchangeCells(M, permfBackqV, permfBackqH)
    val d =DenseMatrix.horzcat(left, DenseMatrix.vertcat(top, M))
    println(d.toString(Int.MaxValue, Int.MaxValue))
    d.toString(Int.MaxValue, Int.MaxValue)

  }


  //TODO : fix the row exchange to sort in order of query
  def exchangeCells(matrix : DenseMatrix[String], permfBackV: BigInt => Int, permfBackH: BigInt => Int): DenseMatrix[String] = {
    val temp = matrix.copy
    for (i <- 0 until matrix.rows) {
      for (j <- 0 until matrix.cols) {
        temp.update(i, j, matrix.valueAt(permfBackV(i), permfBackH(j)))
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


