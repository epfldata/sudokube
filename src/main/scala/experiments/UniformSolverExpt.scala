package experiments

import core._
import solver._
import util._
import RationalTools._
import breeze.linalg.DenseVector
import SolverTools._
import core.solver.Strategy.{CoMomentFrechet, Strategy}
import frontend.experiments.Tools

import java.io.PrintStream
import scala.reflect.ClassTag


class UniformSolverExpt[T:Fractional:ClassTag](dc: DataCube, val name: String = "UniformSolverExpt") {

  val fileout = new PrintStream(s"expdata/$name.csv")
  val strategies = List(Strategy.CoMomentFrechet) //Strategy.values.toList
  fileout.println("Query, QSize, DOF, NFetchTime(us), UFetchTime(us), " + strategies.map(a => s"$a AddTime(us), $a SolveTime(us), $a Err").mkString(", "))
  println("Uniform Solver of type " + implicitly[ClassTag[T]])

  //def online_compare(q: List[Int]) = {
  //  println("Query = " + q)
  //  Profiler.resetAll()
  //  val naiveRes = dc.naive_eval(q)
  //
  //  val solvers = Strategy.values.map(a => new UniformSolver[T](q.length, a))
  //  val l = Profiler("USolve Prepare") {
  //    dc.m.prepare(q, q.length - 1, q.length - 1)
  //  }
  //  Profiler("USolve Fetch") {
  //    l.foreach { pm =>
  //      println(s"Fetching ${pm.accessible_bits}")
  //      val fetched = dc.fetch2[T](List(pm))
  //      //TODO: Change to IndexedSeq
  //      s.add(pm.accessible_bits, fetched.toArray)
  //      val solverRes = s.fastSolve()
  //      val err = error(naiveRes, s.solution)
  //      println("Error = " + err)
  //    }
  //  }
  //  (naiveRes, s.solution.toArray)
  //}

  def fastSum(naive: Array[Double]): Array[Double] = {
    val result = naive.clone()
    val N = naive.size
    var h = 1
    while (h < N) {
      (0 until N by h * 2).foreach { i =>
        (i until i + h).foreach { j =>
          val sum = result(j) + result(j + h)
          result(j) = sum
        }
      }
      h *= 2
    }
    result
  }
  def uniform_solve(q: Seq[Int]) = {

    val l = Profiler("USolve Prepare") {
      dc.m.prepare(q, q.length - 1, q.length - 1)
    }
    val solvers = strategies.map(a => new UniformSolver[T](q.length, a))

    val fetched =  Profiler("USolve Fetch") {
     l.map { pm =>
       (pm.accessible_bits, dc.fetch2[T](List(pm)).toArray)
      }
    }
    val result = solvers.map { s =>
      Profiler(s"USolve Add ${s.strategy}") {
        fetched.foreach{ case (bits, array) => s.add(bits, array)}
      }

      Profiler(s"USolve FillMissing ${s.strategy}") {
        s.fillMissing()
      }
      val res = Profiler(s"USolve Solve ${s.strategy}") {
        s.fastSolve()
      }
      s
    }
    result
  }

  def compare(qu: Seq[Int]) = {
    val q = qu.sorted
    println("Query = " + qu)
    Profiler.resetAll()
    val naiveRes = dc.naive_eval(q)
    val naiveCum = fastSum(naiveRes)

    val solverRes = uniform_solve(q)
    val num = implicitly[Fractional[T]]
    //Profiler.print()


    val errors = solverRes.map{s => s.strategy ->  error(naiveRes, s.solution)}
    val dof = solverRes.head.dof
    val knownSums = solverRes.head.knownSums

    println("DOF = " + dof)
    println("Errors = " + errors.map(_._2).mkString("   "))

    val prec = 1000.0
    //val allcums = naiveCum.indices.map { i =>
    //  val n = (naiveCum(i) * prec/ naiveCum(0)).toInt
    //  val ssv = solverRes.map(s => num.toInt(num.times(num.fromInt(prec.toInt), num.div(s.sumValues(i), s.sumValues(0)))))
    //  (i, n, ssv)
    //}
    //allcums.sortBy{case (i, n, ssv) => Math.abs(ssv.head - n)}.map{case (i, n, ssv) => s"$i ${knownSums(i)} ==> $n     ${ssv.mkString(" ")}"}.foreach(println)

      val nfetch = Profiler.durations("NaiveFetch")._2/1000
      val ufetch = Profiler.durations("USolve Fetch")._2/1000

      def round(v: Double) = {
        val prec = 10000
        math.floor(v*prec)/prec
      }

      val resultrow = s"${qu.mkString(":")},${q.size},$dof,$nfetch, $ufetch," +
        errors.map{case (algo, err) =>
          val uadd = Profiler.durations(s"USolve Add $algo")._2/1000
          val usolve= Profiler.durations(s"USolve Solve $algo")._2/1000
          s"${uadd},${usolve},${round(err)}"
        }.mkString(", ")
    fileout.println(resultrow)

  }

  def rnd_compare(qsize: Int) = {
    println("Query size = "+qsize)
    val query = Tools.rand_q(dc.m.n_bits, qsize)
    compare(query)
  }
}

object UniformSolverExpt {
  def main(args: Array[String]) = {
   //val dc = core.DataCube.load2("Iowa200k_cols6_0.1_1.4")
   val dc = core.DataCube.load2("Iowa200k_volL_2p-195_2")
    import SloppyFractionalInt._
   val expt = new UniformSolverExpt[Rational](dc)
    val volumeGallon = List(0, 23, 45, 74, 96, 126, 163, 195, 208)
    val bottleSold = Nil //List(7, 30, 64, 103, 139, 179, 193, 208)
    val bottleRetail =  List(19, 42, 52, 76, 91, 118, 133, 146, 168, 188)
    val query = volumeGallon //(bottleSold ++ bottleRetail).sorted
    //val query = List(73, 123, 127, 193)
   //val query = List(0, 18, 39, 42, 45)
    expt.compare(query)
    ()
    //var line = io.StdIn.readLine("\nEnter Query : ")
    //while(line != "stop") {
    //  try {
    //    val query = line.split(" ").map(_.toInt).toList.sorted
    //    expt.compare(query)
    //  } catch {
    //    case ex: Throwable => ex.getStackTrace.take(10).foreach(println)
    //  }
    //  line = io.StdIn.readLine("\n Enter Query : ")
    //}
  }
}

object UniformSolverExptRandom {
  def main(args: Array[String]) = {
    //val dc = core.DataCube.load2("Iowa200k_cols6_0.1_1.4")
    //val dc = core.DataCube.load2("Iowa200k_cols6_2p-30_2")
    val dc = core.DataCube.load2("Iowa200k_sales_2p-193_2")
    //val dc = core.DataCube.load2("Random-10")
    val expt = new UniformSolverExpt(dc)
    val iters = 100
    (4 to 12).foreach{qsize => (0 until iters).foreach(_ => expt.rnd_compare(qsize))}

    //var line = io.StdIn.readLine("\nEnter QuerySize : ")
    //while(line != "stop") {
    //  try {
    //    val querysize = line.toInt
    //    expt.rnd_compare(querysize)
    //  } catch {
    //    case ex: Throwable => ex.getStackTrace.take(10).foreach(println)
    //  }
    //  line = io.StdIn.readLine("\n Enter QuerySize : ")
    //}


  }
}