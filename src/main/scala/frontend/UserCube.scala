package frontend

import backend.CBackend
import breeze.linalg.{Axis, DenseMatrix}
import core.solver.MomentSolverAll
import core.{DataCube, RandomizedMaterializationScheme2}
import TestLine.testLineOp
import frontend.schema.Schema
import util.Bits

import scala.annotation.tailrec

class UserCube(val cube: DataCube, val sch: Schema) {

  /**
   * saves the datacube into a file
   *
   * @param filename : the file to save into
   */
  def save(filename: String): Unit = {
    cube.save2(filename)
    sch.save(filename)
  }


  /**
   * recursively search for the first thresh bits of a specific value
   *
   * @param field  : the field to consider
   * @param thresh : maximum number of bits to collect
   * @param n      : the index of the current bit
   * @param acc    : accumlator of the bits numbers
   * @return the first thresh bits of a specific field, or a Nil
   */
  @tailrec
  final def accCorrespondingBits(field: String, thresh: Int, n: Int, acc: List[Int]): List[Int] = {
    if (n < sch.n_bits && acc.size < thresh) {
      if (sch.decode_dim(List(n)).head.map(x => x.split("[= ]").apply(0)).head.equals(field)) {
        accCorrespondingBits(field, thresh, n + 1, acc ::: List(n))
      } else {
        accCorrespondingBits(field, thresh, n + 1, acc)
      }
    } else {
      acc
    }
  }
  /**
   * simple query aggregation, without slicing
   *
   * @param qV     query to display vertically, in the form (field to consider, on n bits)
   * @param qH     query to display horizontally, in the form (field to consider, on n bits) (as to be Nil for the
   *               tuples resultForms
   * @param method method of query, naive or moment
   * @param resultForm Allows to choose to return a matrix, an array, a tuple with bits displayed or a tuple with the prefixes displayed
   * @return reconstructed matrix, with headers
   */
  def query(qV: List[(String, Int, List[String])], qH: List[(String, Int, List[String])], operator: Operator, method: Method, resultForm: ResultForm): Any = {
    val queryBitsV = qV.map(x => accCorrespondingBits(x._1, x._2, 0, Nil))
    val queryBitsH = qH.map(x => accCorrespondingBits(x._1, x._2, 0, Nil))
    val q_sorted = (queryBitsV.flatten ++ queryBitsH.flatten).sorted
    var resultArray: Array[Any] = Array.empty
    method match {
      case NAIVE => resultArray = cube.naive_eval(q_sorted).map(b => b)
      case MOMENT => resultArray = momentMethod(q_sorted)
    }
    resultForm match {
      case MATRIX => createResultMatrix(qV.map(x => (x._1, x._3)), qH.map(x => (x._1, x._3)), queryBitsV, queryBitsH, operator, resultArray)
      case ARRAY => ArrayFunctions.createResultArray(sch, qV.map(x => (x._1, x._3)), qH.map(x => (x._1, x._3)), queryBitsV, queryBitsH, operator, resultArray)
      case TUPLES_BIT => ArrayFunctions.createTuplesBit(sch, qV.map(x => (x._1, x._3)),
        queryBitsV,
        operator,
        resultArray)
      case TUPLES_PREFIX => ArrayFunctions.createTuplesPrefix(sch, qV.map(x => (x._1, x._3)), queryBitsV ++ queryBitsH,
        operator, resultArray)
    }
  }

  /**
   * function used to aggregate, instead of the fact, the values of another dimension (discarding the null facts)
   * @param q the base dimension (X)
   * @param aggregateDim the dimension we want to aggregate (Y), has to be a number cell
   * @param method the method of the query, naive or by moment
   * @return
   */
  def queryDimension(q: (String, Int), aggregateDim: String, method: Method): Any = {
    val queryBits = List(accCorrespondingBits(q._1, q._2, 0, Nil))
    val queryBitsTarget = List(accCorrespondingBits(aggregateDim, Int.MaxValue, 0, Nil))
    val q_sorted = (queryBits.flatten ++ queryBitsTarget.flatten).sorted
    var resultArray: Array[Any] = Array.empty
    method match {
      case NAIVE => resultArray = cube.naive_eval(q_sorted).map(b => b)
      case MOMENT => resultArray = momentMethod(q_sorted)
    }
    val resultArrayTuple = ArrayFunctions.createTuplesPrefix(sch, Nil, (queryBitsTarget ++ queryBits), OR, resultArray)
      .map(x => x.asInstanceOf[(String, Any)]).filter(x => x._2 != "0.0")
    if (aggregateDim == null) { //in this case simply take the fact
      resultArrayTuple.groupBy(x => ArrayFunctions.findValueOfPrefix(x._1, q._1, true)).map(x =>
        (q._1 + "=" + x._1, x._2.foldLeft(0.0)((acc, x) =>
          acc + x._2.toString.toDouble
        ))
      )
    } else {
      resultArrayTuple.groupBy(x => ArrayFunctions.findValueOfPrefix(x._1, q._1, true)).map(x =>
        (q._1 + "=" + x._1, x._2.foldLeft(0.0)((acc, x) =>
          acc + ArrayFunctions.findValueOfPrefix(x._1, aggregateDim, false).toDouble
        ))
      )
    }
  }

  /**
   * function used to aggregate, instead of the fact, the values of another dimension (discarding the null facts) and then check if the facts are increasing
   * or decreasing monotonically
   * @param src source facts, the values of the queryDimension map
   * @param tolerance threshold after which the map is not montonic anymore
   * @return
   */
  def queryDimensionMonotonic(q: (String, Int), aggregateDim: String, method: Method, tolerance: Double): Boolean = {
    val values = queryDimension(q, aggregateDim, method).asInstanceOf[Map[String, Any]].values.map(x => x.toString.toDouble)
    print(values)
    true
  }
  /**
   * util method to solve query with moment method
   *
   * @param q_sorted bits of query, sorted
   * @return array of result, raw
   */
  def momentMethod(q_sorted: List[Int]): Array[Any] = {
    var moment_method_result: Array[Double] = Array.empty

    def callback(s: MomentSolverAll[Double]) = {
      moment_method_result = s.solution
      true
    }

    cube.online_agg_moment(q_sorted, 2, callback)
    moment_method_result.map(b => b.toString)
  }

  /**
   * create result matrix and reorder the result to conform to the query order
   * @param qV  bits of query vertically
   * @param qH  bits of query horizontally
   * @param src source array, to transform in matrix
   * @return DenseMatrix concatenated with top and left headers
   */
  def createResultMatrix(sliceV: List[(String, List[String])], sliceH: List[(String, List[String])], qV: List[List[Int]], qH: List[List[Int]], op: Operator, src: Array[Any]): DenseMatrix[String] = {
    val bH = qH.flatten.size
    val bV = qV.flatten.size

    //first permute the query result, globally sorted, into the global order given
    val q_unsorted = (qV.flatten ++ qH.flatten)
    val q_sorted = q_unsorted.sorted
    val perm = q_unsorted.map(b => q_sorted.indexOf(b)).toArray
    val permf = Bits.permute_bits(q_unsorted.size, perm)

    //then permute the headers for vertical and horizontal
    val permBackqV = qV.flatten.sorted.map(b => qV.flatten.indexOf(b)).toArray
    val permfBackqV = Bits.permute_bits(qV.flatten.size, permBackqV)
    val permBackqH = qH.flatten.sorted.map(b => qH.flatten.indexOf(b)).toArray
    val permfBackqH = Bits.permute_bits(qH.flatten.size, permBackqH)
    var M = new DenseMatrix[String](1 << bV, 1 << bH)
    for (i <- 0 until M.rows) {
      for (j <- 0 until M.cols) {
        M(i, j) = src(permf(j * M.rows + i)).toString
      }
    }

    //prepare the top header, and select the columns to slice if necessary
    var top = DenseMatrix.zeros[String](1, M.cols)
    var linesExcludedH: List[Int] = Nil
    if (qH.nonEmpty) {
      sch.decode_dim(qH.flatten.sorted).zipWithIndex.foreach(pair => {
        val newValue = pair._1.mkString(";").replace(" in List", "=")
        if (testLineOp(op, newValue.split(";").sorted, sliceH)) {
          linesExcludedH = permfBackqH(pair._2) :: linesExcludedH
        } else {
          top(0, permfBackqH(pair._2)) = newValue
        }
      })
    }

    //prepare the left header, and select the rows to discard if necessary
    var left: DenseMatrix[String] = DenseMatrix.zeros[String](M.rows + 1, 1)
    left(0, 0) = ""
    var linesExcludedV: List[Int] = Nil
    if (qV.nonEmpty) {
      sch.decode_dim(qV.flatten.sorted)
        .zipWithIndex.foreach(pair => {
        val newValue = pair._1.mkString(";").replace(" in List", "=")
        if (testLineOp(op, newValue.split(";").sorted, sliceV)) {
          linesExcludedV = permfBackqV(pair._2) :: linesExcludedV
        } else {
          left(permfBackqV(pair._2) + 1, 0) = newValue
        }
      })
    }
    //discard the selected columns/rows
    M = M.delete(linesExcludedV, Axis._0)
    M = M.delete(linesExcludedH, Axis._1)
    left = left.delete(linesExcludedV.map(i => i + 1), Axis._0)
    top = top.delete(linesExcludedH, Axis._1)

    //case if all rows/columns have been deleted
    if (M.rows == 0 || M.cols == 0) {
      return DenseMatrix.zeros[String](1, 1)
    }

    //treat each specific case separately
    if (qV.isEmpty && qH.isEmpty) {
      M
    } else if (qV.isEmpty) {
      DenseMatrix.vertcat(top, M)
    } else if (qH.isEmpty) {
      //delete the top header if it contains only one category
      DenseMatrix.horzcat(left, DenseMatrix.vertcat(top, M)).delete(0, Axis._0)
    } else {
      DenseMatrix.horzcat(left, DenseMatrix.vertcat(top, M))
    }

  }

  /**
   * method created to aggregate and slice on different values
   * @param aggregateColumns columns to aggregate
   * @param sliceColumns columns to slice
   * @param op operator for slice, AND or OR (for testLine method)
   * @param method method for query, moment or naive
   * @return
   */
  def aggregateAndSlice(aggregateColumns: List[(String, Int)], sliceColumns: List[(String, List[String])], op: Operator, method: Method): Array[Any] = {
    val queryBits = aggregateColumns.map(x => accCorrespondingBits(x._1, x._2, 0, Nil))
    val sliceBits = sliceColumns.map(x => accCorrespondingBits(x._1, sch.n_bits, 0, Nil))
    val q_unsorted = (queryBits.flatten ++ sliceBits.flatten)
    var resultArray: Array[Any] = Array.empty
    method match {
      case NAIVE => resultArray = cube.naive_eval(q_unsorted.sorted).map(b => b)
      case MOMENT => resultArray = momentMethod(q_unsorted.sorted)
    }
    val res = ArrayFunctions.createTuplesPrefix(sch, sliceColumns, queryBits ++ sliceBits, op, resultArray)
    res
  }


}

object UserCube {

  /**
   * create a UserCube from a saved file
   *
   * @param filename name of the file to load from
   * @return a new UserCube
   */
  def load(filename: String): UserCube = {
    val cube = DataCube.load2(filename)
    val schema = Schema.load(filename)
    new UserCube(cube, schema)
  }

  /**
   * create a UserCube from a json file
   *
   * @param filename        name of the JSON file
   * @param fieldToConsider field to consider as aggregate value
   * @return a new UserCube
   */
  def createFromJson(filename: String, fieldToConsider: String): UserCube = {
    val sch = new schema.DynamicSchema
    val R = sch.read(filename, Some(fieldToConsider), _.asInstanceOf[Int].toLong)
    val matScheme = RandomizedMaterializationScheme2(sch.n_bits, 8, 4, 4)
    val dc = new DataCube(matScheme)
    dc.build(CBackend.b.mk(sch.n_bits, R.toIterator))
    new UserCube(dc, sch)
  }
}
