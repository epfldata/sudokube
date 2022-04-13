package frontend

import frontend.UserCube.testLine
import frontend.schema.Schema
import util.Bits

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ArrayFunctions {


  /**
   * @param sch Schema of the cube
   * @param qV  bits of query vertically
   * @param qH  bits of query horizontally
   * @param src source array, to transform in matrix
   * @return densematrix decomposed, in forme (array for the top header, array of the left header, values for cells)
   */
  def createResultArray(sch: Schema,sliceV: List[(String, List[String])], sliceH: List[(String, List[String])], qV: List[List[Int]], qH: List[List[Int]], src: Array[String]): (Array[String], Array[String], Array[String]) = {
    val cols = 1 << qH.flatten.size
    val rows = 1 << qV.flatten.size

    val q_unsorted = (qV.flatten ++ qH.flatten)
    val q_sorted = q_unsorted.sorted
    val perm = q_unsorted.map(b => q_sorted.indexOf(b)).toArray
    val permf = Bits.permute_bits(q_unsorted.size, perm)

    val permBackqV = qV.flatten.sorted.map(b => qV.flatten.indexOf(b)).toArray
    val permfBackqV = Bits.permute_bits(qV.flatten.size, permBackqV)
    val permBackqH = qH.flatten.sorted.map(b => qH.reverse.flatten.indexOf(b)).toArray
    val permfBackqH = Bits.permute_bits(qH.flatten.size, permBackqH)

    val resultArray = new Array[String](cols * rows)
    for (i <- 0 until rows) {
      for (j <- 0 until cols) {
        resultArray(i*cols+j) = src(permf(j * rows + i))
      }
    }

    var linesExcludedH: List[Int] = Nil
    val top = new Array[String](cols)
    if (qH.nonEmpty) {
      sch.decode_dim(qH.flatten.sorted).zipWithIndex.foreach(pair => {
        val newValue = pair._1.mkString(";").replace(" in List", "=")
        if (testLine(newValue.split(";").sorted, sliceH, 0)) {
          linesExcludedH = permfBackqH(pair._2) :: linesExcludedH
        } else {
          top(permfBackqH(pair._2)) = newValue
        }
      })
    }

    val left = new Array[String](rows)
    var linesExcludedV: List[Int] = Nil
    if (qV.nonEmpty) {
      sch.decode_dim(qV.flatten.sorted)
        .zipWithIndex.foreach(pair => {
        val newValue = pair._1.mkString(";").replace(" in List", "=")
        if (testLine(newValue.split(";").sorted, sliceV, 0)) {
          linesExcludedV = permfBackqV(pair._2) :: linesExcludedV
        } else {
          left(permfBackqV(pair._2)) = newValue}})
    }

    (top, left, deleteRowsCols(linesExcludedV, linesExcludedH, rows, cols, resultArray))
  }

  def deleteRowsCols(rowsExcluded: List[Int], colsExcluded: List[Int], rows: Int, cols: Int, src: Array[String]): Array[String] = {
    var temp: List[String] = Nil
    for (i<- 0 until rows) {
      for (j<- 0 until cols) {
        if (!rowsExcluded.contains(i) && !colsExcluded.contains(j))
        temp = temp ::: List(src(i*cols + j))
      }
    }
    temp.toArray
  }

}
