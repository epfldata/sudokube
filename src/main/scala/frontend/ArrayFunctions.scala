package frontend

import frontend.TestLine.testLineOp
import frontend.schema.Schema
import util.BitUtils._

import scala.annotation.tailrec

object ArrayFunctions {

  /**
   * decompose a source bit into one bit for each of q_sorted
   * (transform [0, 0, 1, 0, 1] and [1, 3, 5, 6, 9] into List(b1=0,b3=0,b5=1,b6=0,b9=1)
   * @param src array of the value for each bit of q_sorted
   * @param q_sorted array of the bits queried, with length corresponding to src
   * @param acc List to accumulate the results for each index of src
   * @return a full decomposition of the bits for a single cell
   */
  @tailrec
  private def decomposeBits(src: Array[Char], q_sorted: IndexedSeq[Int], acc: List[String]): List[String] = {
    src match {
      case Array() => acc
      case _ => decomposeBits(src.tail, q_sorted.tail, acc ::: List("b%d=%s".format(q_sorted.head, src.head)))
    }
  }

  /**
   * decompose an int in binary form, with number of digits equal to digits param
   * @param source the integer we want to transform into a binary string
   * @param digits the number of digits of this binary string (may need to add leading zeros)
   * @return a string of the source integer converted into a binary string of length digits
   */
  private def asNdigitBinary(source: Int, digits: Int): String = {
    val l: java.lang.Long = source.toBinaryString.toLong
    String.format("%0" + digits + "d", l)
  }

  /**
   * decompose each bit, and returns its index and value for each fact
   * @param sch schema, used to retrieve the prefix values, to slice
   * @param sliceV values to slice
   * @param qV query bits
   * @param op operator for slice (AND or OR)
   * @param src source array, from cuboid query
   * @return array in form of (b1=0;b2=1;b7=0,fact) (String, Any)
   */
  def createTuplesBit(sch: Schema, sliceV: IndexedSeq[(String, List[String])],
                      qV: IndexedSeq[IndexedSeq[Int]], op: OPERATOR,
                      src: Array[Any]): Array[Any] = {
    val rows = 1 << qV.flatten.size
    val q_sorted = (qV.flatten).sorted

    val srcWithIndexes = new Array[Any](rows)
    src.indices.par.foreach{i =>
      val charArray = asNdigitBinary(i, rows.toBinaryString.length - 1).toCharArray //assign to each elem of the src array a binary digit
      srcWithIndexes(i) = (decomposeBits(charArray, q_sorted, Nil), src(i))
    }
    val res = createResultArray(sch, sliceV, IndexedSeq.empty, qV, IndexedSeq.empty, op, srcWithIndexes)
    res._3
  }

  /**
   * transform source array in form (prefix1=x1;prefix2=x2;prefix7=(x1, x2),fact) (STring, Any)
   * @param sch schema to retrieve the prefixes for bits
   * @param sliceV the strings to slice
   * @param qV the query bits
   * @param op the operator for slicing(AND or OR)
   * @param src source data from query
   * @return array in form (prefix1=x1;prefix2=x2;prefix7=(x1, x2),fact) (STring, Any)
   */
  def createTuplesPrefix(sch: Schema, sliceV: IndexedSeq[(String, List[String])], qV: IndexedSeq[IndexedSeq[Int]], op: OPERATOR, src: Array[Any]): Array[Any] = {
    val res = createResultArray(sch, sliceV, IndexedSeq.empty, qV, IndexedSeq.empty, op, src)
    val cols = res._1.length
    val rows = res._2.length
    for (i <- 0 until rows) {
      for (j <- 0 until cols) {
        res._3(i * cols + j) = (res._1(j) + res._2(i), res._3(i * cols + j)) //for each cell, create a tuple of its horizontal and vertical header added
      }
    }
    res._3
  }

  /**
   * create result array and reorder the result to conform to the query order
   *
   * @param sch Schema of the cube
   * @param qV  bits of query vertically
   * @param qH  bits of query horizontally
   * @param src source array, to transform in matrix
   * @return dense matrix decomposed, in form (array for the top header, array of the left header, values for cells)
   */
  def createResultArray(sch: Schema, sliceV: IndexedSeq[(String, List[String])], sliceH: IndexedSeq[(String, List[String])], qV: IndexedSeq[IndexedSeq[Int]], qH: IndexedSeq[IndexedSeq[Int]], op: OPERATOR, src: Array[Any]): (Array[String], Array[String], Array[Any]) = {
    val cols = 1 << qH.flatten.size
    val rows = 1 << qV.flatten.size

    //functions to reorder the values, with the order provided by the query
    val q_unsorted = (qV.flatten ++ qH.flatten)
    val q_sorted = q_unsorted.sorted
    val perm = q_unsorted.map(b => q_sorted.indexOf(b)).toArray
    val permf = permute_bits(q_unsorted.size, perm)

    //functions to reorder the headers, in the same fashion
    val permBackqV = qV.flatten.sorted.map(b => qV.flatten.indexOf(b)).toArray
    val permfBackqV = permute_bits(qV.flatten.size, permBackqV)
    val permBackqH = qH.flatten.sorted.map(b => qH.reverse.flatten.indexOf(b)).toArray
    val permfBackqH = permute_bits(qH.flatten.size, permBackqH)

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
          linesExcludedH = permfBackqH(pair._2) :: linesExcludedH //accumulate the indexes of the excluded lines
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
        if (testLineOp(op, newValue.split(";"), sliceV)) {
          linesExcludedV = permfBackqV(pair._2) :: linesExcludedV //accumulate the indexes of the excluded lines
        } else {
          left(permfBackqV(pair._2)) = newValue
        }
      })
    }

    //return the result in decomposed format
    (top.indices.collect { case i if !linesExcludedH.contains((i)) => top(i) }.toArray.map(i => if (i == null) "" else i),
      left.indices.collect { case i if !linesExcludedV.contains((i)) => left(i) }.toArray.map(i => if (i == null) "" else i),
      deleteRowsCols(linesExcludedV, linesExcludedH, rows, cols, resultArray))
  }

  /**
   * method to reconstruct ann Array without the discarded rows and columns
   *
   * @param rowsExcluded List of index of the excluded rows
   * @param colsExcluded List of index of the excluded columns
   * @param rows         number of rows contained in the array
   * @param cols         number of columns contained in the array
   * @param src          source array, not sliced yet
   * @return array containing the same elements of the src array, in the same order, but for the discarded rows and columns
   */
  private def deleteRowsCols(rowsExcluded: List[Int], colsExcluded: List[Int], rows: Int, cols: Int, src: Array[Any]): Array[Any] = {
    var temp: List[Any] = Nil
    for (i <- 0 until rows) {
      for (j <- 0 until cols) {
        if (!rowsExcluded.contains(i) && !colsExcluded.contains(j)) {
          temp = temp ::: List(src(i * cols + j)) //only add the values of non-excluded rows and cols
        }
      }
    }
    temp.toArray
  }

  /**
   * simple function that takes an array in the tuple form (either binary or prefix) and simply delete all the tuples that contain zero as second value
   */
  def deleteZeroColumns(source: Array[(String, Any)]): Array[(String, Any)] = {
    source.filter(_._2.toString.toDouble != 0.0)
  }

  /**
   * given a source string, in the form (prefix1=x;prefix2=y...) or (prefix1=(x1, x2, x3);prefix2=y;prefix3=(z1, z2)...),
   * and given a prefix existing in the src string, returns the value of the src for this prefix
   *
   * @param src    source string in the form (prefix1=x;prefix2=y...) or (prefix1=(x1, x2, x3);prefix2=y;prefix3=(z1, z2)...)
   * @param prefix prefix string, if it is not contained in src, returns " "
   */
  def findValueOfPrefix(src: String, prefix: String, multipleValue: Boolean): String = {
    //retrieve value of prefix parameter
    try {
      val values = src.split(";").filter(s => s.contains(prefix))(0).split("=")(1)
      //if multiple values for a row, take the first one
      if (values.contains("(") && !multipleValue) {
        //drop the first parenthesis and return only the first value
        values.split(",")(0).drop(1)
      } else {
        //return the whole set of values
        values
      }
    } catch {
      case _: ArrayIndexOutOfBoundsException => " "
    }
  }

  /**
   * performs window based aggregates on tuplePrefix
   * We assume that the array is sorted by the dim_prefix column
   */
  def windowAggregate(source: Array[Any], dim_prefix: String, gap: Int, window_type: WINDOW): Array[(String, Any)] = {
    if (!source(0).asInstanceOf[(String, Any)]._1.contains(dim_prefix)) {
      return null
    }
    build_aggregate_row(window_type, dim_prefix, source.map(x => x.asInstanceOf[(String, Any)]), gap, 0, Array.empty)
  }

  /**
   * used to accumulate the values of rows, for window based aggregate based on number of rows
   * @param source
   * @param n
   * @param max
   * @param acc
   * @return
   */
  private def accumulateRows(source: Array[(String, Any)], n: Int, max: Int, acc: (String, Any)): (String, Any) = {
    if (n == max) {
      acc
    } else {
      val add = (acc._1, acc._2.toString.toDouble + source(n)._2.toString.toDouble)
      accumulateRows(source, n + 1, max, add)
    }
  }

  /**
   * used to accumulate the values of rows, for window based aggregate based on values of rows
   * @param source
   * @param prefix
   * @param n
   * @param max
   * @param acc
   * @return
   */
  private def accumulateRowsValues(source: Array[(String, Any)], prefix: String, n: Int, max: Int, acc: (String, Any)): (String, Any) = {
    if (n == source.length) {
      acc
    } else {
      if (findValueOfPrefix(source(n)._1, prefix, false).toInt > max) {
        acc
      } else {
        val add = (acc._1, acc._2.toString.toDouble + source(n)._2.toString.toDouble)
        accumulateRowsValues(source, prefix, n + 1, max, add)
      }
    }
  }

  /**
   * this function helps to build a window aggregate, by calling the appropriate function for each line recursively
   *
   * @param window_type either select a certain number of rows, or a certain window of values starting from the value at n
   * @param prefix      the prefix we want to observe for the window aggregate
   * @param source      the source matrix, in a tuplePrefix form ((prefix1=x;prefix2=y...), value)
   * @param gap         if NUM_ROWS, the number of rows we want to select ; if VALUES_ROWS, the maximal difference for a row to be selected
   * @param n           the index we currently aggregate from
   * @param acc         the return values, recurively accumulated
   * @return the acc function when we have iterated over the whole source
   */
  private def build_aggregate_row(window_type: WINDOW, prefix: String, source: Array[(String, Any)], gap: Int, n: Int, acc: Array[(String, Any)]): Array[(String, Any)] = {
    if (n == source.length) {
      acc
    } else {
      window_type match {
        case NUM_ROWS => build_aggregate_row(window_type, prefix, source, gap, n + 1, acc :+ accumulateRows(source, n, Math.min(source.length - 1, n + gap), (source(n)._1, 0.0)))
        case VALUES_ROWS => build_aggregate_row(window_type, prefix, source, gap, n + 1, acc :+ accumulateRowsValues(source, prefix, n, gap + findValueOfPrefix(source(n)._1, prefix, false).toInt, (source(n)._1, 0.0)))
        case _ => null
      }
    }
  }

  /**
   * apply a binary function to each row of the source, and then reduce the result using forall or exist
   *
   * @param source   source array, in form TuplesPrefix
   * @param function the binary function to apply to 2 columns values
   * @param prefixes tuple containing the 2 prefixes, in order, of the 2 columns needed for the binary function
   * @param method   booleanMethod to reduce the result, forall or exist(all true or one true)
   * @param n        as this method is tail-recursive, n is index of the tuple currently treated
   * @return one boolean result
   */
  def applyBinary(source: Array[(String, Any)], function: (Any, Any) => Boolean, prefixes: (String, String), method: BOOL_METHOD, n: Int = 0): Boolean = {
    if (source.length == n) {
      method match {
        case FORALL => true
        case EXIST => false
      }
    } else {
      if (function(findValueOfPrefix(source(n)._1, prefixes._1, false), findValueOfPrefix(source(n)._1, prefixes._2, false))) { //test the binary function
        method match {
          case FORALL => applyBinary(source, function, prefixes, method, n + 1)
          case EXIST => true
        }
      } else {
        method match {
          case FORALL => false
          case EXIST => applyBinary(source, function, prefixes, method, n + 1)
        }
      }
    }
  }

  /**
   * compute the slope and the offset (intercept) of an (x,y) array
   * @param source an (x, y) array, consisting of doubles
   * @return (average slope, offset) of the curve
   */
  def slopeAndIntercept(source: Seq[(Double, Double)]): (Double, Double) = {
    val xavg = source.map(x => x._1).sum/source.length //compute average of x
    val yavg = source.map(x => x._2).sum/source.length //compute average of y
    val xycovar = source.map(x => (x._1 - xavg)*(x._2 - yavg)).sum //covar of x,y
    val xxvar = source.map(x => (x._1 - xavg)*(x._1 - xavg)).sum //var of x
    (xycovar/xxvar, yavg - (xycovar/xxvar)*xavg) //compute slope and intercept
  }

  /**
   * util function to evaluate the slope between two points
   * @param x point at time x
   * @param y point at index x+1
   * @param precision value up to which the slope between 2 points is considered flat
   * @return 0 if the slope is flat, -1 if it is decreasing and 1 if it is increasing
   */
  private def ~=(x: Double, y: Double, precision: Double) = {
    if ((x - y).abs < precision || x == y) { //not increasing nor decreasing (or not much
      0
    } else if ((x - y) > 0) { //decreasing greatly
      -1
    } else { //increasing greatly
      1
    }
  }

  /**
   * counts the number of monoticity breaks in a Seq of values
   * @param items the seq considered
   * @param tolerance value up to which it is not considered a monotonicity break (sort of smoothing)
   * @return number of monotonicity breaks
   */
  def findMonotonicityBreaks(items: Seq[Double], tolerance: Double): Int = {
    items.sliding(2). // two items at a time // remove any equalities
      map{p => ~=(p.head, p(1), tolerance)}.filter(_ != 0). // get -1 and 1 values; must have 2 values; add 'smoothing' tolerance
      sliding(2). // take *these* two at a time
      count{p => p.size == 2 && p.head != p(1)}
  }


}
