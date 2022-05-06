package frontend

import frontend.TestLine.testLineOp
import frontend.schema.Schema
import util.{BigBinary, Bits}

import java.util
import scala.:+
import scala.annotation.tailrec
import scala.collection.mutable

object ArrayFunctions {


  /**
   * create result array and reorder the result to conform to the query order
   * @param sch Schema of the cube
   * @param qV  bits of query vertically
   * @param qH  bits of query horizontally
   * @param src source array, to transform in matrix
   * @return densematrix decomposed, in form (array for the top header, array of the left header, values for cells)
   */
  def createResultArray(sch: Schema, sliceV: List[(String, List[String])], sliceH: List[(String, List[String])], qV: List[List[Int]], qH: List[List[Int]], op : Operator,src: Array[Any]): (Array[String], Array[String], Array[Any]) = {
    val cols = 1 << qH.flatten.size
    val rows = 1 << qV.flatten.size

    //functions to reorder the values, with the order provided by the query
    val q_unsorted = (qV.flatten ++ qH.flatten)
    val q_sorted = q_unsorted.sorted
    val perm = q_unsorted.map(b => q_sorted.indexOf(b)).toArray
    val permf = Bits.permute_bits(q_unsorted.size, perm)

    //functions to reorder the headers, in the same fashion
    val permBackqV = qV.flatten.sorted.map(b => qV.flatten.indexOf(b)).toArray
    val permfBackqV = Bits.permute_bits(qV.flatten.size, permBackqV)
    val permBackqH = qH.flatten.sorted.map(b => qH.reverse.flatten.indexOf(b)).toArray
    val permfBackqH = Bits.permute_bits(qH.flatten.size, permBackqH)

    //fill in the resultArray with the correct values
    val resultArray = new Array[Any](cols * rows)
    for (i <- 0 until rows) {
      for (j <- 0 until cols) {
        resultArray(i * cols + j) = src(permf(j * rows + i))
      }
    }

    //constructs the top header and may slice some columns if necessary
    var linesExcludedH: List[Int] = Nil
    val top = new Array[String](cols)
    if (qH.nonEmpty) {
      sch.decode_dim(qH.flatten.sorted).zipWithIndex.foreach(pair => {
        val newValue = pair._1.mkString(";").replace(" in List", "=")
        if (testLineOp(op, newValue.split(";").sorted, sliceH)) {
          linesExcludedH = permfBackqH(pair._2) :: linesExcludedH
        } else {
          top(permfBackqH(pair._2)) = newValue
        }
      })
    }

    //construct the left header and may slice some rows if necessary
    val left = new Array[String](rows)
    var linesExcludedV: List[Int] = Nil
    if (qV.nonEmpty) {
      sch.decode_dim(qV.flatten.sorted)
        .zipWithIndex.foreach(pair => {
        val newValue = pair._1.mkString(";").replace(" in List", "=")
        if (testLineOp(op, newValue.split(";").sorted, sliceV)) {
          linesExcludedV = permfBackqV(pair._2) :: linesExcludedV
        } else {
          left(permfBackqV(pair._2)) = newValue
        }
      })
    }

    //return the result in decomposed format
    (top.indices.collect { case i if !linesExcludedH.contains((i)) => top(i) }.toArray.map(i => if(i == null) "" else i), left.indices.collect { case i if !linesExcludedV.contains((i)) => left(i) }.toArray.map(i => if(i == null) "" else i), deleteRowsCols(linesExcludedV, linesExcludedH, rows, cols, resultArray))
  }

  /**
   * method to reconstruct ann Array without the discarded rows and columns
   * @param rowsExcluded List of index of the excluded rows
   * @param colsExcluded List of index of the excluded columns
   * @param rows number of rows contained in the array
   * @param cols number of columns contained in the array
   * @param src source array, not sliced yet
   * @return array containing the same elements of the src array, in the same order, but for the discarded rows and columns
   */
  private def deleteRowsCols(rowsExcluded: List[Int], colsExcluded: List[Int], rows: Int, cols: Int, src: Array[Any]): Array[Any] = {
    var temp: List[Any] = Nil
    for (i <- 0 until rows) {
      for (j <- 0 until cols) {
        if (!rowsExcluded.contains(i) && !colsExcluded.contains(j)) {
          temp = temp ::: List(src(i * cols + j))
        }
      }
    }
    temp.toArray
  }

  def createTuplesBit(sch: Schema, sliceV: List[(String, List[String])], qV: List[List[Int]], op : Operator,src: Array[Any]): Array[Any] = {
    val rows = 1 << qV.flatten.size

    //functions to reorder the values, with the order provided by the query
    val q_unsorted = qV.flatten
    val q_sorted = q_unsorted.sorted

    val srcWithIndexes = new Array[Any](rows)
    for (i <- src.indices) {
      val charArray = asNdigitBinary(i, (rows).toBinaryString.length-1).toCharArray
      srcWithIndexes(i) = (decomposeBits(charArray, Nil, q_sorted), src(i))
    }
    val res = createResultArray(sch, sliceV, Nil, qV, Nil, op, srcWithIndexes)
    res._3
  }

  def createTuplesPrefix(sch: Schema, sliceV: List[(String, List[String])], qV: List[List[Int]], op : Operator,src: Array[Any]): Array[Any] = {
    val res = createResultArray(sch, sliceV, Nil, qV, Nil, op, src)
    val cols = res._1.length
    val rows = res._2.length
    for (i <- 0 until rows) {
      for (j <- 0 until cols) {
        res._3(i * cols + j) = (res._1(j) + res._2(i), res._3(i * cols + j))
      }
    }
    res._3
  }

  @tailrec
  private def decomposeBits(src: Array[Char], acc: List[String], q_sorted: List[Int]): List[String] = {
    src match {
      case Array() => acc
      case _ => decomposeBits(src.tail, acc ::: List("b%d=%s".format(q_sorted.head, src.head)), q_sorted.tail)
    }
  }

  private def asNdigitBinary(source: Int, digits: Int): String = {
    val l: java.lang.Long = source.toBinaryString.toLong
    String.format("%0" + digits + "d", l)
  }

  private def accumulateRows(source: Array[(String, Any)], n: Int, max: Int, acc: (String, Any)): (String, Any) = {
    if (n == max) {
      acc
    } else {
      val add = (acc._1, acc._2.toString.toDouble + source(n)._2.toString.toDouble)
      accumulateRows(source, n+1, max, add)
    }
  }

  private def accumulateRowsValues(source: Array[(String, Any)], n: Int, max: Int, acc: (String, Any)): (String, Any) = {
    if (source(n)._2.asInstanceOf[Double] >= max) {
      acc
    } else {
      val add = (acc._1, acc._2.asInstanceOf[Double] + source(n)._2.asInstanceOf[Double])
      accumulateRows(source, n+1, max, add)
    }
  }

  private def build_aggregate_row(window_type: WINDOW, source: Array[(String, Any)], gap: Int, n: Int, acc: Array[(String, Any)]): Array[(String, Any)]= {
    if (n == source.length) {
      acc
    } else {
      window_type match {
        case NUM_ROWS => build_aggregate_row(window_type, source, gap, n+1, acc :+ accumulateRows(source, n, Math.min(source.length, n+gap), (source(n)._1, 0.0)))
        case VALUES_ROWS => build_aggregate_row(window_type, source, gap, n+1, acc :+ accumulateRowsValues(source, n, Math.min(source.length, n+gap), (source(n)._1, 0.0)))
        case _ => null
      }
    }
  }

  /**
   * performs window based aggregates on tuplePrefix
   * We assume that the array is sorted by the dim_prefix column
   */
  def window_aggregate(source: Array[Any], dim_prefix: String,gap: Int, window_type: WINDOW): Array[(String, Any)] = {
    if (!source(0).asInstanceOf[(String, Any)]._1.contains(dim_prefix)) {
      return null
    }
    build_aggregate_row(window_type, source.map(x => x.asInstanceOf[(String, Any)]), gap, 0, Array.empty)
  }



}
