package experiments

import core._
import solver._
import util._
import RationalTools._
import breeze.linalg.DenseVector
import SolverTools._
import core.solver.Strategy.Strategy
import frontend.experiments.Tools

import java.io.PrintStream
import scala.reflect.ClassTag


class UniformSolverExpt[T:Fractional:ClassTag](dc: DataCube) {

  val fileout = new PrintStream("expdata/UniformSolverExpt.csv")
  val strategies = List(Strategy.CoMoment) //Strategy.values.toList
  fileout.println("QSize, DOF, NFetch, UFetch, " + strategies.map(a => s"USolve Add $a, USolve Solve $a, USolve ErrMax $a, USolve Err $a").mkString(", "))
  println("Uniform Solver of type "+ implicitly[ClassTag[T]])

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

  def uniform_solve(q: List[Int]) = {

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

      val res = Profiler(s"USolve Solve ${s.strategy}") {
        s.fastSolve()
      }
      println("Total = "+ res.sum)
      //s.verifySolution()
      s.strategy -> (s.solution, s.errMax, s.dof)
    }
    result
  }

  def compare(q: List[Int]) = {
    println("Query = " + q)
    Profiler.resetAll()
    val naiveRes = dc.naive_eval(q)
    val solverRes = uniform_solve(q)

    //Profiler.print()

    val errors = solverRes.map{case (algo, (sol, emax, _)) => algo -> (emax, error(naiveRes, sol))}
    val dof = solverRes.head._2._3
    println("DOF = " + dof)
    println("Errors = " + errors.map(_._2).mkString("   "))
    val total = naiveRes.sum
    val prec =  1000.0
    println("Naive = " +  naiveRes.takeRight(10).map(v => (v * prec /total).toInt/prec).mkString(" "))
    solverRes.foreach {case (algo, (sol, _, _)) => println(s"USolve ${algo} = " +  sol.takeRight(10).map(v => (v * prec /total).toInt/prec).mkString(" "))}
    println("\n")

      val nfetch = Profiler.durations("NaiveFetch")._2/1000
      val ufetch = Profiler.durations("USolve Fetch")._2/1000

      def round(v: Double) = {
        val prec = 10000
        math.floor(v*prec)/prec
      }

      val resultrow = s"${q.size},$dof,$nfetch, $ufetch," +
        errors.map{case (algo, (errMax, err)) =>
          val uadd = Profiler.durations(s"USolve Add $algo")._2/1000
          val usolve= Profiler.durations(s"USolve Solve $algo")._2/1000
          s"${uadd},${usolve}, ${round(errMax)}, ${round(err)}"
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
    //val dc = core.DataCube.load2("Iowa200k_sales_2p-193_2")
    val dc = core.DataCube.load2("Random-10")
    val expt = new UniformSolverExpt(dc)
    val iters = 1
    (4 to 4).foreach{qsize => (0 until iters).foreach(_ => expt.rnd_compare(qsize))}

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