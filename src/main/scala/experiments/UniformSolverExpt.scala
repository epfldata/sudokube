package experiments

import core._
import solver._
import util._
import RationalTools._
import breeze.linalg.DenseVector
import SolverTools._
import frontend.experiments.Tools

import scala.reflect.ClassTag


class UniformSolverExpt[T:Fractional:ClassTag](dc: DataCube) {

  println("Uniform Solver of type "+ implicitly[ClassTag[T]])
  def online_compare(q: List[Int]) = {
    println("Query = " + q)
    Profiler.resetAll()
    val naiveRes = dc.naive_eval(q)

    val s = new UniformSolver[T](q.length)
    val l = Profiler("USolve Prepare") {
      dc.m.prepare(q, q.length - 1, q.length - 1)
    }
    Profiler("USolve Fetch") {
      l.foreach { pm =>
        println(s"Fetching ${pm.accessible_bits}")
        val fetched = dc.fetch2[T](List(pm))
        //TODO: Change to IndexedSeq
        s.add(pm.accessible_bits, fetched.toArray)
        val solverRes = s.solve()
        val err = error(naiveRes, solverRes.toArray)
        println("Error = " + err)
      }
    }
    (naiveRes, s.solution.toArray)
  }

  def uniform_solve(q: List[Int]) = {
    val s = new UniformSolver[T](q.length)
    val l = Profiler("USolve Prepare") {
      dc.m.prepare(q, q.length - 1, q.length - 1)
    }
    Profiler("USolve Fetch") {
      l.foreach { pm =>
        val fetched = dc.fetch2[T](List(pm))
        //TODO: Change to IndexedSeq
        s.add(pm.accessible_bits, fetched.toArray)
      }
    }
    val result = Profiler("USolve solve") {
      s.solve()
    }
    s.verifySolution()
    result
  }


  def compare(q: List[Int]) = {
    println("Query = " + q)
    Profiler.resetAll()
    val naiveRes = dc.naive_eval(q)
    val solverRes = uniform_solve(q)
    Profiler.print()
    val err = error(naiveRes, solverRes.toArray)
    println("Error = " + err)
    (naiveRes, solverRes.toArray)
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
   val dc = core.DataCube.load2("Iowa200k_cols6_2p-30_2")
    import SloppyFractionalInt._
   val expt = new UniformSolverExpt[Rational](dc)
   val query = List(2, 10, 23, 32, 35, 46)
    val (res1, res2) = expt.compare(query)
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
    val dc = core.DataCube.load2("Iowa200k_cols6_2p-30_2")
    val expt = new UniformSolverExpt(dc)

    var line = io.StdIn.readLine("\nEnter QuerySize : ")
    while(line != "stop") {
      try {
        val querysize = line.toInt
        expt.rnd_compare(querysize)
      } catch {
        case ex: Throwable => ex.getStackTrace.take(10).foreach(println)
      }
      line = io.StdIn.readLine("\n Enter QuerySize : ")
    }


  }
}