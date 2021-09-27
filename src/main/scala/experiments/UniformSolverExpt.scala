package experiments

import core._
import solver._
import util._
import RationalTools._
import breeze.linalg.DenseVector
import frontend.experiments.Tools


class UniformSolverExpt(dc: DataCube) {

  def uniform_solve(q: List[Int]) = {
    val s = new UniformSolver[Rational](q.length)
    val l = Profiler("USolve Prepare") {
      dc.m.prepare(q, q.length - 1, q.length - 1)
    }
    Profiler("USolve Fetch") {
      l.foreach { pm =>
        val fetched = dc.fetch2(List(pm))
        //TODO: Change to IndexedSeq
        s.add(pm.accessible_bits, fetched.toArray)
      }
    }
    val result = Profiler("USolve solve") {
      s.solve()
    }
    result
  }

  def error(naive: Array[Double], solver: DenseVector[Double]) = {
    val length = naive.length
    val deviation = (0 until length).map(i => Math.abs(naive(i) - solver(i))).sum
    val sum = naive.sum
    deviation / sum
  }

  def compare(q: List[Int]) = {
    Profiler.resetAll()
    val naiveRes = dc.naive_eval(q)
    val solverRes = uniform_solve(q)
    Profiler.print()
    val err = error(naiveRes, solverRes)
    println("Error = " + err)
  }

  def rnd_compare(qsize: Int) = {
    val query = Tools.rand_q(dc.m.n_bits, qsize)
    println("Query = " + query)
    compare(query)
  }
}

object UniformSolverExpt {
  def main(args: Array[String]) = {
   val dc = core.DataCube.load2("Iowa200k_cols6_0.1_1.4")
   val expt = new UniformSolverExpt(dc)
    var line = io.StdIn.readLine("Query : ")
    while(line != "stop") {
      try {
        val query = line.split(" ").map(_.toInt).toList
        expt.compare(query)
      } catch {
        case ex: Throwable => ex.getStackTrace.take(10).foreach(println)
      }
      line = io.StdIn.readLine("\n Enter Query : ")
    }
  }
}
