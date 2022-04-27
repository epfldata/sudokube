package frontend

import backend.CBackend
import breeze.linalg.{Axis, DenseMatrix}
import core.solver.MomentSolverAll
import core.{DataCube, RandomizedMaterializationScheme2}
import frontend.UserCube.testLineAnd
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
   * @param qH     query to display horizontally, in the form (field to consider, on n bits)
   * @param method method of query, naive or moment
   * @return reconstructed matrix, with headers
   */
  def queryMatrix(qV: List[(String, Int)], qH: List[(String, Int)], method: String): DenseMatrix[String] = {
    val queryBitsV = getBitsForField(qV)
    val queryBitsH = getBitsForField(qH)
    val q_sorted = (queryBitsV.flatten ++ queryBitsH.flatten).sorted
    var resultArray: Array[String] = Array.empty
    method match {
      case "naive" => resultArray = cube.naive_eval(q_sorted).map(b => b.toString)
      case "moment" => resultArray = momentMethod(q_sorted)
    }
    createResultMatrix(qV.map(x => (x._1, Nil)), qH.map(x => (x._1, Nil)), queryBitsV, queryBitsH, resultArray)
  }

  /**
   * simple query aggregation, may include slicing
   *
   * @param qV     query to display vertically, in the form (field to consider, on n bits)
   * @param qH     query to display horizontally, in the form (field to consider, on n bits)
   * @param method method of query, naive or moment
   * @return reconstructed matrix, with headers
   */
  def querySliceMatrix(qV: List[(String, Int, List[String])], qH: List[(String, Int, List[String])], method: String): DenseMatrix[String] = {
    val queryBitsV = getBitsForFieldS(qV)
    val queryBitsH = getBitsForFieldS(qH)
    val q_sorted = (queryBitsV.flatten ++ queryBitsH.flatten).sorted
    var resultArray: Array[String] = Array.empty
    method match {
      case "naive" => resultArray = cube.naive_eval(q_sorted).map(b => b.toString)
      case "moment" => resultArray = momentMethod(q_sorted)
    }
    createResultMatrix(qV.map(x => (x._1, x._3)), qH.map(x => (x._1, x._3)), queryBitsV, queryBitsH, resultArray)
  }

  def getBitsForFieldS(input: List[(String, Int, List[String])]): List[List[Int]] = {
    input.map(x => accCorrespondingBits(x._1, x._2, 0, Nil))
  }

  def getBitsForField(input: List[(String, Int)]): List[List[Int]] = {
    input.map(x => accCorrespondingBits(x._1, x._2, 0, Nil))
  }

  /**
   * simple query aggregation, without slicing
   *
   * @param qV     query to display vertically, in the form (field to consider, on n bits)
   * @param qH     query to display horizontally, in the form (field to consider, on n bits)
   * @param method method of query, naive or moment
   * @return densematrix decomposed, in form (array for the top header, array of the left header, values for cells)
   */
  def queryArray(qV: List[(String, Int)], qH: List[(String, Int)], method: String): (Array[String], Array[String], Array[String]) = {
    val queryBitsV = getBitsForField(qV)
    val queryBitsH = getBitsForField(qH)
    val q_sorted = (queryBitsV.flatten ++ queryBitsH.flatten).sorted
    var resultArray: Array[String] = Array.empty
    method match {
      case "naive" => resultArray = cube.naive_eval(q_sorted).map(b => b.toString)
      case "moment" => resultArray = momentMethod(q_sorted)
    }
    ArrayFunctions.createResultArray(sch, qV.map(x => (x._1, Nil)), qH.map(x => (x._1, Nil)), queryBitsV, queryBitsH, resultArray)
  }

  /**
   * simple query aggregation, with slicing
   *
   * @param qV     query to display vertically, in the form (field to consider, on n bits)
   * @param qH     query to display horizontally, in the form (field to consider, on n bits)
   * @param method method of query, naive or moment
   * @return densematrix decomposed, in form (array for the top header, array of the left header, values for cells)
   */
  def queryArrayS(qV: List[(String, Int, List[String])], qH: List[(String, Int, List[String])], method: String): (Array[String], Array[String], Array[String]) = {
    val queryBitsV = qV.map(x => accCorrespondingBits(x._1, x._2, 0, Nil))
    val queryBitsH = qH.map(x => accCorrespondingBits(x._1, x._2, 0, Nil))
    val q_sorted = (queryBitsV.flatten ++ queryBitsH.flatten).sorted
    var resultArray: Array[String] = Array.empty
    method match {
      case "naive" => resultArray = cube.naive_eval(q_sorted).map(b => b.toString)
      case "moment" => resultArray = momentMethod(q_sorted)
    }
    ArrayFunctions.createResultArray(sch, qV.map(x => (x._1, x._3)), qH.map(x => (x._1, x._3)), queryBitsV, queryBitsH, resultArray)
  }

  /**
   * util method to solve query with moment method
   *
   * @param q_sorted bits of query, sorted
   * @return array of result, raw
   */
  def momentMethod(q_sorted: List[Int]): Array[String] = {
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
  def createResultMatrix(sliceV: List[(String, List[String])], sliceH: List[(String, List[String])], qV: List[List[Int]], qH: List[List[Int]], src: Array[String]): DenseMatrix[String] = {
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
        M(i, j) = src(permf(j * M.rows + i))
      }
    }

    //prepare the top header, and select the columns to slice if necessary
    var top = DenseMatrix.zeros[String](1, M.cols)
    var linesExcludedH: List[Int] = Nil
    if (qH.nonEmpty) {
      sch.decode_dim(qH.flatten.sorted).zipWithIndex.foreach(pair => {
        val newValue = pair._1.mkString(";").replace(" in List", "=")
        if (testLineAnd(newValue.split(";").sorted, sliceH, 0)) {
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
        if (testLineAnd(newValue.split(";").sorted, sliceV, 0)) {
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

  /**
   * recursively checks if the provided string checks all the criteria of qV_sorted
   *
   * @param splitString string to test
   * @param q_sorted   criteria, in form (field, list of acceptable values)
   * @param n           index of field considered
   * @return true <=> one of the condition is not fulfilled
   */
  def testLineAnd(splitString: Array[String], q_sorted: List[(String, List[String])], n: Int): Boolean = {
    if (n != splitString.length) {
      if (q_sorted(n)._2.isEmpty) {
        return testLineAnd(splitString, q_sorted, n + 1)
      }
      for (i <- q_sorted(n)._2.indices) {
        if (splitString(n).contains(q_sorted(n)._2(i))) {
          return testLineAnd(splitString, q_sorted, n + 1)
        }
      }
      true

    } else {
      false
    }
  }

  /**
   * recursively checks if the provided string checks one of the criteria of qV_sorted
   *
   * @param splitString string to test
   * @param q_sorted   criteria, in form (field, list of acceptable values)
   * @param n           index of field considered
   * @return true <=> all the conditions are not fulfilled
   */
  def testLineOr(splitString: Array[String], q_sorted: List[(String, List[String])], n: Int): Boolean = {
    if (n != splitString.length) {
      if (q_sorted(n)._2.isEmpty) {
        return testLineOr(splitString, q_sorted, n + 1)
      }
      for (i <- q_sorted(n)._2.indices) {
        if (splitString(n).contains(q_sorted(n)._2(i))) {
          return testLineOr(splitString, q_sorted, n + 1)
        }
      }
      true

    } else {
      false
    }
  }
}
