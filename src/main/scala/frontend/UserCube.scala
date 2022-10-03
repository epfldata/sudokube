package frontend

import backend.CBackend
import core.DataCube
import TestLine.testLineOp
import core.materialization.RandomizedMaterializationStrategy
import frontend.schema.Schema
import util.BitUtils.permute_bits

import scala.annotation.tailrec
import breeze.linalg.{Axis, DenseMatrix}
import core.solver.moment.MomentSolverAll


sealed class METHOD
case object MOMENT extends METHOD
case object NAIVE extends METHOD

sealed class BOOL_METHOD
case object EXIST extends BOOL_METHOD
case object FORALL extends BOOL_METHOD

sealed class OPERATOR
case object AND extends OPERATOR
case object OR extends OPERATOR

sealed class RESULT_FORM
case object MATRIX extends RESULT_FORM
case object ARRAY extends RESULT_FORM
case object TUPLES_BIT extends RESULT_FORM
case object TUPLES_PREFIX extends RESULT_FORM

sealed class WINDOW
case object NUM_ROWS extends WINDOW
case object VALUES_ROWS extends WINDOW



class UserCube(val cubeName: String, val cube: DataCube, val sch: Schema) {

  /**
   * saves the datacube into a file
   *
   * @param filename : the file to save into
   */
  def save(): Unit = {
    cube.save()
    sch.save(cubeName)
  }


  /**
   * recursively search for the first thresh bits of a specific value
   *
   * @param field  : the field to consider
   * @param thresh : maximum number of bits to collect
   * @param n      : the index of the current bit
   * @param acc    : accumulator of the bits numbers
   * @return the first thresh bits of a specific field, or a Nil
   */
  @tailrec
  final def accCorrespondingBits(field: String, thresh: Int, acc: List[Int], n: Int = sch.n_bits - 1): List[Int] = {
    if (n >= 0 && acc.size < thresh) {
      if (sch.decode_dim(Vector(n)).head.map(x => x.split("[= ]").apply(0)).head.equals(field)) {
        accCorrespondingBits(field, thresh, n :: acc, n - 1)
      } else {
        accCorrespondingBits(field, thresh, acc, n - 1)
      }
    } else {
      acc
    }
  }
  /**
   * List(("Region", List("China", "India")), ("spicy", List("45"))) //select (Region = China || Region = India) && spicy = 45
   *
   * @param qV     query to display vertically, in the form (field to consider, on n bits, values to slice (if Nil, all values are accepted))
   * @param qH     query to display horizontally, in the form (field to consider, on n bits, values to slice (if Nil, all values are accepted)) (as to be Nil for the
   *               tuples resultForms
   * @param method method of query, naive or moment
   * @param resultForm Allows to choose to return a matrix, an array, a tuple with bits displayed or a tuple with the prefixes displayed
   * @param operator operator to apply, for query List(("Region", List("China", "India")), ("spicy", List("45"))) with operator op
   *                 it will be translated to select (REGION = China || Region = India) op spicy = 45
   *                 we can also apply negation (!India will be (REGION = China || Region != India) op spicy = 45)
   *                 and comparison operators prepended at the beginning to numerical values (<, >, <= and >=)
   * @return reconstructed matrix, with headers
   */
  def query(qV: IndexedSeq[(String, Int, List[String])], qH: IndexedSeq[(String, Int, List[String])], operator: OPERATOR = AND, method: METHOD = MOMENT, resultForm: RESULT_FORM): Any = {
    val queryBitsV = qV.map(x => accCorrespondingBits(x._1, x._2, Nil).toIndexedSeq)
    val queryBitsH = qH.map(x => accCorrespondingBits(x._1, x._2, Nil).toIndexedSeq)
    val q_sorted = (queryBitsV.flatten ++ queryBitsH.flatten).sorted
    var resultArray: Array[Any] = Array.empty
    method match {
      case NAIVE => resultArray = cube.naive_eval(q_sorted).map(b => b)
      case MOMENT => resultArray = momentMethod(q_sorted)
    }
    resultForm match {
      case MATRIX => createResultMatrix(qV.map(x => (x._1, x._3)), qH.map(x => (x._1, x._3)), queryBitsV, queryBitsH, operator, resultArray)
      case ARRAY => ArrayFunctions.createResultArray(sch, qV.map(x => (x._1, x._3)), qH.map(x => (x._1, x._3)), queryBitsV, queryBitsH, operator, resultArray)
      case TUPLES_BIT => ArrayFunctions.createTuplesBit(sch, (qV ++ qH).map(x => (x._1, x._3)),
        queryBitsV ++ queryBitsH,
        operator,
        resultArray)
      case TUPLES_PREFIX => ArrayFunctions.createTuplesPrefix(sch, (qV ++ qH).map(x => (x._1, x._3)), queryBitsV ++ queryBitsH,
        operator, resultArray)
    }
  }

  def queryAlt(qV: IndexedSeq[(String, List[String])], qH: IndexedSeq[(String, List[String])], operator: OPERATOR = AND, method: METHOD = MOMENT, resultForm: RESULT_FORM = TUPLES_PREFIX): Any = {
    val result = query(qV.map(x => (x._1, sch.n_bits, x._2)), qH.map(x => (x._1, sch.n_bits, x._2)), operator, method, resultForm)
    result match {
      case array: Array[(String, Double)] => array.filter(_._2 != 0)
      case _ => result
    }
  }


  /**
   * function used to aggregate, instead of the fact, the values of another dimension (discarding the null facts)
   * @param q the base dimension (X)
   * @param aggregateDim the dimension we want to aggregate (Y), has to be a number dimension (if null, we simply take the normal values)
   * @param method the method of the query, naive or by moment
   * @param groupByMethod function to map the dimension values to facilitate group by (e.g. dates to month of the year)
   * @return
   */
  def queryDimension(q: (String, Int, List[String]), aggregateDim: String, method: METHOD, groupByMethod: String => String = (x => x)): Seq[(String, Double)] = {
    var res: Map[String, Double] = null
    if (aggregateDim == null) { //in this case simply take the fact
      val resultArrayTuple = query(Vector(q), IndexedSeq.empty, AND, method, TUPLES_PREFIX).asInstanceOf[Array[Any]]
        .map(x => x.asInstanceOf[(String, Any)]).filter(x => x._2 != "0.0")
      res = resultArrayTuple.map(x => (groupByMethod(ArrayFunctions.findValueOfPrefix(x._1, q._1, true)), x._2)).groupBy(x => x._1)
        .map(value => (value._1, value._2.foldLeft(0.0)((acc, x) =>
          acc + x._2.toString.toDouble
        )))
    } else {
      //TODO: SBJ : Naively converted List to vector. Check if q can be added at the end
      val resultArrayTuple = query(q +: Vector((aggregateDim, sch.n_bits, Nil)), IndexedSeq.empty, AND, method, TUPLES_PREFIX).asInstanceOf[Array[Any]] //refactor the aggregateDim param to be able to use the query function
        .map(x => x.asInstanceOf[(String, Any)]).filter(x => x._2 != "0.0")
      res = resultArrayTuple.map(x => (groupByMethod(ArrayFunctions.findValueOfPrefix(x._1, q._1, true)), ArrayFunctions.findValueOfPrefix(x._1, aggregateDim, false))).
        groupBy(x => x._1).map(value =>
        (value._1, value._2.foldLeft(0.0)((acc, x) =>
          acc + x._2.toString.toDouble
        ))
      )
    }
    try {
      res.toSeq.sortBy(_._1.toDouble)
    } catch {
      case e: NumberFormatException => res.toSeq.sortBy(_._1)
    }
  }

  /**
   * function used to check if the facts obtained with query_dimension are increasing
   * or decreasing monotonically
   * @param tolerance threshold after which the map is not montonic anymore
   * @return
   */
  def queryDimensionMonotonic(q: (String, Int, List[String]), aggregateDim: String, method: METHOD, tolerance: Double): Boolean = {
    ArrayFunctions.findMonotonicityBreaks(queryDimension(q, aggregateDim, method).asInstanceOf[Vector[(String, Any)]].map(x => x._2.toString.toDouble), tolerance) == 0
  }

  /**
   * function used to check if the facts obtained with query_dimension has a double peak (i.e. has at least 3 monotonicity breaks)
   * @param tolerance threshold after which the map is not montonic anymore
   * @return
   */
  def queryDimensionDoublePeak(q: (String, Int, List[String]), aggregateDim: String, method: METHOD, tolerance: Double): Boolean = {
    ArrayFunctions.findMonotonicityBreaks(queryDimension(q, aggregateDim, method).asInstanceOf[Vector[(String, Any)]].map(x => x._2.toString.toDouble), tolerance) >= 3
  }


  /**
   * util method to solve query with moment method
   *
   * @param q_sorted bits of query, sorted
   * @return array of result, raw
   */
  def momentMethod(q_sorted: IndexedSeq[Int]): Array[Any] = {
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
  def createResultMatrix(sliceV: IndexedSeq[(String, List[String])], sliceH: IndexedSeq[(String, List[String])], qV: IndexedSeq[IndexedSeq[Int]], qH: IndexedSeq[IndexedSeq[Int]], op: OPERATOR, src: Array[Any]): DenseMatrix[String] = {
    val bH = qH.flatten.size
    val bV = qV.flatten.size

    //first permute the query result, globally sorted, into the global order given
    val q_unsorted = (qV.flatten ++ qH.flatten)
    val q_sorted = q_unsorted.sorted
    val perm = q_unsorted.map(b => q_sorted.indexOf(b)).toArray
    val permf = permute_bits(q_unsorted.size, perm)

    //then permute the headers for vertical and horizontal
    val permBackqV = qV.flatten.sorted.map(b => qV.flatten.indexOf(b)).toArray
    val permfBackqV = permute_bits(qV.flatten.size, permBackqV)
    val permBackqH = qH.flatten.sorted.map(b => qH.flatten.indexOf(b)).toArray
    val permfBackqH = permute_bits(qH.flatten.size, permBackqH)
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

}

object UserCube {
  implicit val backend = CBackend.default
  /**
   * create a UserCube from a saved file
   *
   * @param cubeName name of the file to load from
   * @return a new UserCube
   */
  def load(cubeName: String): UserCube = {
    val cube = DataCube.load(cubeName)
    val schema = Schema.load(cubeName)
    new UserCube(cubeName, cube, schema)
  }

  /**
   * create a UserCube from a json file
   *
   * @param filename        name of the JSON file
   * @param fieldToConsider field to consider as aggregate value
   * @return a new UserCube
   */
  def createFromJson(filename: String, fieldToConsider: String, cubeName: String): UserCube = {
    val sch = new schema.DynamicSchema
    val R = sch.read(filename, Some(fieldToConsider), x => x.toString.toLong)
    val m = new RandomizedMaterializationStrategy(sch.n_bits, 8, 4) //8, 4 numbers can be optimized
    val dc = new DataCube(cubeName)
    val baseCuboid = backend.mk(sch.n_bits, R.toIterator)
    dc.build(baseCuboid, m)
    new UserCube(cubeName, dc, sch)
  }
}
