package frontend

import backend.CBackend
import breeze.linalg.{Axis, DenseMatrix}
import core.solver.MomentSolverAll
import core.{DataCube, Interval, RandomizedMaterializationScheme2, Rational, SparseSolver}
import frontend.schema.Schema
import util.Bits

import scala.annotation.tailrec

class UserCube(val cube: DataCube, val sch : Schema) {


  def save(filename: String): Unit = {
    cube.save2(filename)
    sch.save(filename)
  }

  @tailrec
  private def accCorrespondingBits(field: String, thresh: Int, n : Int, acc: List[Int]): List[Int] = {
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

  def querySlice(qV: List[(String, Int, List[String])], qH: List[(String, Int, List[String])], method: String) : DenseMatrix[String] = {
    var matrix = query(qV.map(x => (x._1, x._2)), qH.map(x => (x._1, x._2)), method)
    matrix = matrix.delete(sliceV(qV.map(x => (x._1, x._3)).sortBy(x=>x._1), matrix, 1, Nil), Axis._0)
    matrix.delete(sliceH(qH.map(x => (x._1, x._3)).sortBy(x=>x._1), matrix, 1, Nil), Axis._1)
  }

  @tailrec
  private def sliceH(qH_sorted: List[(String, List[String])], matrix: DenseMatrix[String], n:Int, acc: List[Int]): List[Int] = {

    if (n == matrix.cols) {
      acc
    } else {
      val splitString = matrix(0, n).split(";").sorted
      if (!testLine(splitString, qH_sorted, 0)) {
        sliceH(qH_sorted, matrix, n + 1, acc)
      } else {
        sliceH(qH_sorted, matrix, n + 1, n :: acc)
      }
    }
  }

  @tailrec
  private def sliceV(qV_sorted: List[(String, List[String])], matrix: DenseMatrix[String], n:Int, acc: List[Int]): List[Int] = {

    if (n == matrix.rows) {
      acc
    } else {
      val splitString = matrix(n, 0).split(";").sorted
      if (!testLine(splitString, qV_sorted, 0)) {
        sliceV(qV_sorted, matrix, n + 1, acc)
      } else {
        sliceV(qV_sorted, matrix, n + 1, n :: acc)
      }
    }
  }


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

  def query(qV: List[(String, Int)], qH: List[(String, Int)], method: String) : DenseMatrix[String] = {
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

  def momentMethod(q_sorted: List[Int]):Array[String] = {
    var moment_method_result: Array[Double] = Array.empty
    def callback(s: MomentSolverAll[Double]) = {
      moment_method_result = s.solution
      true
    }
    cube.online_agg_moment(q_sorted, 2, callback)
    moment_method_result.map(b => b.toString)
  }

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


    M = exchangeCells(M, permfBackqV, permfBackqH)
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

}

object UserCube {
  def load(filename: String): UserCube = {
    val cube = DataCube.load2(filename)
    val schema = Schema.load(filename)
    new UserCube(cube, schema)
  }

  def createFromJson(filename: String, fieldToConsider: String): UserCube = {
    val sch = new schema.DynamicSchema
    val R = sch.read(filename, Some(fieldToConsider), _.asInstanceOf[Int].toLong)
    val matScheme = RandomizedMaterializationScheme2(sch.n_bits, 8, 4, 4)
    val dc = new DataCube(matScheme)
    dc.build(CBackend.b.mk(sch.n_bits, R.toIterator))
    new UserCube(dc, sch)
  }
}
