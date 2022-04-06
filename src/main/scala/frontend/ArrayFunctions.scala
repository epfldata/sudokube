package frontend

import frontend.schema.Schema
import util.Bits

object ArrayFunctions {


  /**
   * @param sch Schema of the cube
   * @param qV  bits of query vertically
   * @param qH  bits of query horizontally
   * @param src source array, to transform in matrix
   * @return densematrix decomposed, in forme (array for the top header, array of the left header, values for cells)
   */
  def createResultArray(sch: Schema, qV: List[List[Int]], qH: List[List[Int]], src: Array[String]): (Array[String], Array[String], Array[String]) = {
    val cols = 1 << qH.flatten.size
    val rows = 1 << qV.flatten.size

    val q_unsorted = (qV.flatten ++ qH.flatten)
    val q_sorted = q_unsorted.sorted
    val perm = q_unsorted.map(b => q_sorted.indexOf(b)).toArray
    val permf = Bits.permute_bits(q_unsorted.size, perm)

    val permBackqV = qV.flatten.sorted.map(b => qV.reverse.flatten.indexOf(b)).toArray
    val permfBackqV = Bits.permute_bits(qV.flatten.size, permBackqV)
    val permBackqH = qH.flatten.sorted.map(b => qH.reverse.flatten.indexOf(b)).toArray
    val permfBackqH = Bits.permute_bits(qH.flatten.size, permBackqH)

    val resultArray = new Array[String](cols * rows)
    for (i <- 0 until rows) {
      for (j <- 0 until cols) {
        resultArray(i * cols + j) = src(permf(j * rows + i))
      }
    }


    val top = new Array[String](cols)
    if (qH.nonEmpty) {
      sch.decode_dim(qH.flatten.sorted).zipWithIndex.foreach(pair => top(permfBackqH(pair._2)) = pair._1.mkString(";").replace(" in List", "="))
    }

    val left = new Array[String](rows)
    if (qV.nonEmpty) {
      sch.decode_dim(qV.flatten.sorted).zipWithIndex.foreach(pair => left(permfBackqV(pair._2)) = pair._1.mkString(";").replace(" in List", "="))
    }
    (top, left, exchangeCellsArray((cols, rows), resultArray, permfBackqV, permfBackqH))
  }

  /**
   * reorder cells of source matrix according the permfBackV and permfBackH
   *
   * @param cols_rows  dimension of array, cols first, then rows
   * @param src        source array
   * @param permfBackV function of reorder, vertically
   * @param permfBackH function of reorder, horizontally
   * @return
   */
  def exchangeCellsArray(cols_rows: (Int, Int), src: Array[String], permfBackV: BigInt => Int, permfBackH: BigInt => Int): Array[String] = {
    val temp = new Array[String](cols_rows._2 * cols_rows._1)
    for (i <- 0 until cols_rows._2) {
      for (j <- 0 until cols_rows._1) {
        temp(i * cols_rows._1 + j) = src(permfBackV(i) * cols_rows._1 + permfBackH(j))
      }
    }
    temp
  }

}
