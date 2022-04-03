package frontend

import backend.CBackend
import breeze.linalg.{Axis, DenseMatrix}
import core.solver.MomentSolverAll
import core.{DataCube, Interval, RandomizedMaterializationScheme2, Rational, SparseSolver}
import frontend.schema.Schema
import util.Bits

import scala.annotation.tailrec

class UserCube(val cube: DataCube, val sch : Schema) {


  /**
   * saves the datacube into a file
   * @param filename: the file to save into
   */
  def save(filename: String): Unit = {
    cube.save2(filename)
    sch.save(filename)
  }


  /**
   * recursively search for the first thresh bits of a specific value
   * @param field: the field to consider
   * @param thresh: maximum number of bits to collect
   * @param n: the index of the current bit
   * @param acc: accumlator of the bits numbers
   * @return the first thresh bits of a specific field, or a Nil
   */
  @tailrec
  final def accCorrespondingBits(field: String, thresh: Int, n : Int, acc: List[Int]): List[Int] = {
    if (n < sch.n_bits && acc.size < thresh) {
      if (sch.decode_dim(List(n)).head.map(x => x.split("[= ]").apply(0)).head.equals(field)) {
        accCorrespondingBits(field, thresh, n+1, acc ::: List(n))
      } else {
        accCorrespondingBits(field, thresh, n+1, acc)
      }
    } else {
      acc
    }
  }

  /**
   * @param qV query to display vertically, in the form (field to consider, on n bits, with list of specific values)
   * @param qH query to display vertically, in the form (field to consider, on n bits, with list of specific values)
   * @param method method of query, naive or moment
   * @return DenseMatrix, final result of query, with headers, in qV and qH order
   */
  def querySliceMatrix(qV: List[(String, Int, List[String])], qH: List[(String, Int, List[String])], method: String) : DenseMatrix[String] = {
    var matrix = queryMatrix(qV.map(x => (x._1, x._2)), qH.map(x => (x._1, x._2)), method)
    matrix = matrix.delete(sliceVMatrix(qV.map(x => (x._1, x._3)).sortBy(x=>x._1), matrix, 1, Nil), Axis._0)
    val result = matrix.delete(sliceHMatrix(qH.map(x => (x._1, x._3)).sortBy(x=>x._1), matrix, 1, Nil), Axis._1)
    if (result.cols == 1 || result.rows == 1) {
      new DenseMatrix[String](1, 1)
    } else {
      result
    }
  }

  /**
   * recursively slice a matrix horizontal header, cell by cell
   * @param qH_sorted specific values to retain for specific fields, in pair
   * @param matrix matrix to slice
   * @param n current cell index
   * @param acc accumulator of cols to delete
   * @return final acc of cols to delete
   */
  @tailrec
  private def sliceHMatrix(qH_sorted: List[(String, List[String])], matrix: DenseMatrix[String], n:Int, acc: List[Int]): List[Int] = {

    if (n == matrix.cols) {
      acc
    } else {
      val splitString = matrix(0, n).split(";").sorted
      if (!testLine(splitString, qH_sorted, 0)) {
        sliceHMatrix(qH_sorted, matrix, n + 1, acc)
      } else {
        sliceHMatrix(qH_sorted, matrix, n + 1, n :: acc)
      }
    }
  }

  /**
   * recursively slice a matrix vertical header, cell by cell
   * @param qV_sorted specific values to retain for specific fields, in pair
   * @param matrix matrix to slice
   * @param n current cell index
   * @param acc accumulator of cols to delete
   * @return final acc of cols to delete
   */
  @tailrec
  private def sliceVMatrix(qV_sorted: List[(String, List[String])], matrix: DenseMatrix[String], n:Int, acc: List[Int]): List[Int] = {

    if (n == matrix.rows) {
      acc
    } else {
      val splitString = matrix(n, 0).split(";").sorted
      if (!testLine(splitString, qV_sorted, 0)) {
        sliceVMatrix(qV_sorted, matrix, n + 1, acc)
      } else {
        sliceVMatrix(qV_sorted, matrix, n + 1, n :: acc)
      }
    }
  }


  /**
   * recursively checks if the provided string checks the criterions of qV_sorted
   * @param splitString string to test
   * @param qV_sorted criterions, in form (field, list of acceptable values)
   * @param n index of field considered
   * @return true <=> all the conditions are fulfilled
   */
  private def testLine(splitString: Array[String], qV_sorted: List[(String, List[String])], n: Int): Boolean = {
    if (n != splitString.length) {
      if (qV_sorted(n)._2.isEmpty) {
        return testLine(splitString, qV_sorted, n+1)
      }
      for (i<- qV_sorted(n)._2.indices) {
        if (splitString(n).contains(qV_sorted(n)._2(i))) {
          return testLine(splitString, qV_sorted, n+1)
        }
      }
      true
    } else {
      false
    }

  }


  /**
   * simple query aggregation, without slicing
   * @param qV query to display vertically, in the form (field to consider, on n bits)
   * @param qH query to display horizontally, in the form (field to consider, on n bits)
   * @param method method of query, naive or moment
   * @return reconstructed matrix, with headers
   */
  def queryMatrix(qV: List[(String, Int)], qH: List[(String, Int)], method: String) : DenseMatrix[String] = {
    val queryBitsV = qV.map(x => accCorrespondingBits(x._1, x._2, 0, Nil))
    val queryBitsH = qH.map(x => accCorrespondingBits(x._1, x._2, 0, Nil))
    val q_sorted = (queryBitsV.flatten ++ queryBitsH.flatten).sorted
    var resultArray: Array[String] = Array.empty
    method match {
      case "naive" => resultArray = cube.naive_eval(q_sorted).map(b => b.toString)
      case "moment" => resultArray = momentMethod(q_sorted)
    }
    val matrix = createResultMatrix(queryBitsV, queryBitsH, resultArray)
    matrix
  }

  /**
   * simple query aggregation, without slicing
   * @param qV query to display vertically, in the form (field to consider, on n bits)
   * @param qH query to display horizontally, in the form (field to consider, on n bits)
   * @param method method of query, naive or moment
   * @return densematrix decomposed, in forme (array for the top header, array of the left header, values for cells)
   */
  def queryArray(qV: List[(String, Int)], qH: List[(String, Int)], method: String) : (Array[String], Array[String], Array[String]) = {
    val queryBitsV = qV.map(x => accCorrespondingBits(x._1, x._2, 0, Nil))
    val queryBitsH = qH.map(x => accCorrespondingBits(x._1, x._2, 0, Nil))
    val q_sorted = (queryBitsV.flatten ++ queryBitsH.flatten).sorted
    var resultArray: Array[String] = Array.empty
    method match {
      case "naive" => resultArray = cube.naive_eval(q_sorted).map(b => b.toString)
      case "moment" => resultArray = momentMethod(q_sorted)
    }
    createResultArray(queryBitsV, queryBitsH, resultArray)
  }

  /**
   * util method to solve query with moment method
   * @param q_sorted bits of query, sorted
   * @return array of result, raw
   */
  def momentMethod(q_sorted: List[Int]):Array[String] = {
    var moment_method_result: Array[Double] = Array.empty
    def callback(s: MomentSolverAll[Double]) = {
      moment_method_result = s.solution
      true
    }
    cube.online_agg_moment(q_sorted, 2, callback)
    moment_method_result.map(b => b.toString)
  }

  /**
   * @param qV bits of query vertically
   * @param qH bits of query horizontally
   * @param src source array, to transform in matrix
   * @return DenseMatrix concatenated with top and left headers
   */
  def createResultMatrix(qV : List[List[Int]], qH : List[List[Int]], src: Array[String]): DenseMatrix[String] = {
    val bH = qH.flatten.size
    val bV = qV.flatten.size



    val q_unsorted = (qV.flatten ++ qH.flatten)
    val q_sorted = q_unsorted.sorted
    val perm = q_unsorted.map(b => q_sorted.indexOf(b)).toArray
    val permf = Bits.permute_bits(q_unsorted.size, perm)

    val permBackqV= qV.flatten.sorted.map(b => qV.reverse.flatten.indexOf(b)).toArray
    val permfBackqV = Bits.permute_bits(qV.flatten.size, permBackqV)
    val permBackqH= qH.flatten.sorted.map(b => qH.reverse.flatten.indexOf(b)).toArray
    val permfBackqH = Bits.permute_bits(qH.flatten.size, permBackqH)

    var M = new DenseMatrix[String](1 << bV, 1 << bH)
    for (i<- 0 until M.rows) {
      for (j<- 0 until M.cols) {
        M(i, j) = src(permf(j*M.rows + i))
      }
    }


    val top = DenseMatrix.zeros[String](1, M.cols)
    if (qH.nonEmpty) {
      sch.decode_dim(qH.flatten.sorted).zipWithIndex.foreach(pair => top(0, permfBackqH(pair._2)) = pair._1.mkString(";").replace(" in List", "="))
    }

    val left: DenseMatrix[String] = DenseMatrix.zeros[String](M.rows + 1, 1)
    left(0, 0) = ""
    if (qV.nonEmpty) {
      sch.decode_dim(qV.flatten.sorted)
        .zipWithIndex.foreach(pair => left(permfBackqV(pair._2) + 1, 0) = pair._1.mkString(";").replace(" in List", "="))
    }


    M = exchangeCellsMatrix(M, permfBackqV, permfBackqH)
    if (qV.isEmpty && qH.isEmpty) {
      M
    } else if (qV.isEmpty) {
      DenseMatrix.vertcat(top, M)
    } else if (qH.isEmpty) {
      DenseMatrix.horzcat(left, DenseMatrix.vertcat(top, M)).delete(0, Axis._0)
    } else {
      DenseMatrix.horzcat(left, DenseMatrix.vertcat(top, M))
    }

  }

  /**
   * @param qV bits of query vertically
   * @param qH bits of query horizontally
   * @param src source array, to transform in matrix
   * @return densematrix decomposed, in forme (array for the top header, array of the left header, values for cells)
   */
  def createResultArray(qV : List[List[Int]], qH : List[List[Int]], src: Array[String]):(Array[String], Array[String], Array[String]) = {
    val cols = 1 << qH.flatten.size
    val rows = 1 << qV.flatten.size

    val q_unsorted = (qV.flatten ++ qH.flatten)
    val q_sorted = q_unsorted.sorted
    val perm = q_unsorted.map(b => q_sorted.indexOf(b)).toArray
    val permf = Bits.permute_bits(q_unsorted.size, perm)

    val permBackqV= qV.flatten.sorted.map(b => qV.reverse.flatten.indexOf(b)).toArray
    val permfBackqV = Bits.permute_bits(qV.flatten.size, permBackqV)
    val permBackqH= qH.flatten.sorted.map(b => qH.reverse.flatten.indexOf(b)).toArray
    val permfBackqH = Bits.permute_bits(qH.flatten.size, permBackqH)

    val resultArray = new Array[String](cols * rows)
    for (i<- 0 until rows) {
      for (j<- 0 until cols) {
        resultArray(i*cols + j) = src(permf(j*rows + i))
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
   * @param matrix source matrix, to reorder
   * @param permfBackV function of reorder, vertically
   * @param permfBackH function of reorder, horizontally
   * @return
   */
  def exchangeCellsMatrix(matrix : DenseMatrix[String], permfBackV: BigInt => Int, permfBackH: BigInt => Int): DenseMatrix[String] = {
    val temp = matrix.copy
    for (i <- 0 until matrix.rows) {
      for (j <- 0 until matrix.cols) {
        temp.update(i, j, matrix.valueAt(permfBackV(i), permfBackH(j)))
      }
    }
    temp
  }

  def exchangeCellsArray(cols_rows : (Int, Int), src : Array[String], permfBackV: BigInt => Int, permfBackH: BigInt => Int): Array[String] = {
    val temp = new Array[String](cols_rows._2 * cols_rows._1)
    for (i <- 0 until cols_rows._2) {
      for (j <- 0 until cols_rows._1) {
        temp(i*cols_rows._1 + j) = src(permfBackV(i)*cols_rows._1 +  permfBackH(j))
      }
    }
    temp
  }

}

object UserCube {

  /**
   * create a UserCube from a saved file
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
   * @param filename name of the JSON file
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
