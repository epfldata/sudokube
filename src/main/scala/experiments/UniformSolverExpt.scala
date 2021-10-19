package experiments

import core._
import solver._
import util._
import RationalTools._
import breeze.linalg.DenseVector
import SolverTools._
import frontend.experiments.Tools

import java.io.PrintStream
import scala.reflect.ClassTag


class UniformSolverExpt[T:Fractional:ClassTag](dc: DataCube) {

  val fileout = new PrintStream("expdata/UniformSolverExpt.csv")
  fileout.println("QSize, NFetch, UFetch1, USolve1, UFetch2, USolve2, DOF, ErrorMax, Err1, Err2")
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

  def uniform_solve1(q: List[Int]) = {
    val s = new UniformSolver[T](q.length)
    s.setSimpleDefault = true
    val l = Profiler("USolve1 Prepare") {
      dc.m.prepare(q, q.length - 1, q.length - 1)
    }
    Profiler("USolve1 Fetch") {
      l.foreach { pm =>
        val fetched = dc.fetch2[T](List(pm))
        //TODO: Change to IndexedSeq
        s.add(pm.accessible_bits, fetched.toArray)
      }
    }
    val result = Profiler("USolve1 solve") {
      s.fastSolve()
    }
    s.verifySolution()
    (s.solution, s.errMax, s.dof)
  }

  def uniform_solve2(q: List[Int]) = {
    val s = new UniformSolver[T](q.length)
    s.setSimpleDefault = false
    val l = Profiler("USolve2 Prepare") {
      dc.m.prepare(q, q.length - 1, q.length - 1)
    }
    Profiler("USolve2 Fetch") {
      l.foreach { pm =>
        val fetched = dc.fetch2[T](List(pm))
        //TODO: Change to IndexedSeq
        s.add(pm.accessible_bits, fetched.toArray)
      }
    }
    val result = Profiler("USolve2 solve") {
      s.fastSolve()
    }
    s.verifySolution()
    s.solution
  }

  def compare(q: List[Int]) = {
    println("Query = " + q)
    Profiler.resetAll()
    val naiveRes = dc.naive_eval(q)
    val (solverRes1,err0, dof) = uniform_solve1(q)
    val solverRes2 = uniform_solve2(q)
    //Profiler.print()
    val err1 = error(naiveRes, solverRes1)
    val err2 = error(naiveRes, solverRes2)
    println("DOF = " + dof)
    println("Error = " + (err0, err1, err2))
    println("Naive = " + naiveRes.take(10).mkString(" ") + naiveRes.takeRight(10).mkString(" "))
    println("USolve1 = " + solverRes1.take(10).mkString(" ") + solverRes1.takeRight(10).mkString(" "))
    println("USolve2 = " + solverRes2.take(10).mkString(" ") + solverRes2.takeRight(10).mkString(" "))
    println("\n")
    val ufetch1 = Profiler.durations("USolve1 Fetch")._2/1000
    val usolve1= Profiler.durations("USolve1 solve")._2/1000
    val ufetch2 = Profiler.durations("USolve2 Fetch")._2/1000
    val usolve2= Profiler.durations("USolve2 solve")._2/1000
    val nfetch = Profiler.durations("NaiveFetch")._2/1000
    fileout.println(s"${q.size},$nfetch, $ufetch1,$usolve1,$ufetch2,$usolve2,$dof," +
      s"${math.floor(err0*1000000)/1000000},${math.floor(err1*1000000)/1000000},${math.floor(err2*1000000)/1000000}")
    (naiveRes, solverRes1, solverRes2)
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
   val query = List(0, 18, 39, 42, 45)
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