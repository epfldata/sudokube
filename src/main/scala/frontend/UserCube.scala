package frontend

import backend.CBackend
import breeze.linalg.DenseMatrix
import core.solver.MomentSolverAll
import core.{DataCube, Interval, RandomizedMaterializationScheme2, Rational, SparseSolver}
import frontend.schema.Schema
import util.Bits

class UserCube(val cube: DataCube, val sch : Schema) {


  def save(filename: String): Unit = {
    cube.save2(filename)
    sch.save(filename)
  }

  def query(qV: List[(String, Int)], qH: List[(String, Int)], method: String) : DenseMatrix[String] = {
    val queryBitsV = qV.flatMap(x => accCorrespondingBits(x._1, x._2, 0, Nil))
    val queryBitsH = qH.flatMap(x => accCorrespondingBits(x._1, x._2, 0, Nil))
    val q_sorted = (queryBitsV ++ queryBitsH).sorted
    var resultArray: Array[String] = Array.empty
    method match {
      case "naive" => resultArray = cube.naive_eval(q_sorted).map(b => b.toString)
      case "moment" => resultArray = momentMethod(q_sorted)
    }
    println(resultArray.mkString(","))
    val matrix = createResultMatrix(queryBitsV, queryBitsH, resultArray)
    println(matrix.toString(Int.MaxValue, Int.MaxValue))
    matrix
  }

  def momentMethod(q_sorted: List[Int]):Array[String] = {
    var moment_method_result: Array[Double] = Array.empty
    def callback(s: MomentSolverAll[Double]) = {
      moment_method_result = s.solution
      true
    }
    cube.online_agg_moment(q_sorted, 2, callback)
    return moment_method_result.map(b => b.toString)
  }

  def createResultMatrix(qV : List[Int], qH : List[Int], src: Array[String]): DenseMatrix[String] = {
    val bH = qH.size
    val bV = qV.size


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
        M(i, j) = src(permf(j*M.rows + i))
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
    d

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

  def accCorrespondingBits(field: String, thresh: Int, n : Int, acc: List[Int]): List[Int] = {
    if (n < sch.n_bits && acc.size < thresh) {
      if (sch.decode_dim(List(n)).head.map(x => x.split("[= ]").apply(0)).head.equals(field)) {
        accCorrespondingBits(field, thresh, n+1, n :: acc)
      } else {
        accCorrespondingBits(field, thresh, n+1, acc)
      }
    } else {
      acc
    }
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
