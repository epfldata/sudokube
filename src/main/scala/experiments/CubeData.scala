package experiments

import core.{Rational, SolverTools, SparseSolver}
import frontend._
import frontend.experiments._
import util.Profiler

import java.io.PrintStream

object CubeData {

  def mkCube(n_row_log: Int) = {
    Profiler.resetAll()
    val name = s"S1_d${n_bits}_n${n_row_log}"
    val nrows = 1L << n_row_log

    val dc = Profiler("mkDC") {
      Tools.mkDC(n_bits, rf, base, nrows, Sampling.f1)
    }
    Profiler("Save DC") {
      dc.save(name)
    }
    Profiler.print()
  }

  val n_bits = 60
  val rf = 0.1
  val base = 1.15
  //(0 to 4).map( i => 20 + i * 5).foreach { n =>
  //  val dcw = Tools.mkDC(n_bits, rf, base,  1L << n, Sampling.f1)
  //}

  def expt(qsize: Int, n_row_log: Int, iters: Int) {
    val cubename = s"S1_d${n_bits}_n${n_row_log}"
    val pw = new PrintStream(s"expdata/expt_${n_bits}_${rf}_${base}_${n_row_log}_${qsize}.csv")
    val cheap_size = qsize * 3
    val dc = Profiler("DC Load") {
      core.DataCube.load(cubename)
    }
    val dcloadtime = Profiler.durations("DC Load")._2
    (0 until iters).foreach { it =>
      Profiler.resetAll()
      val query = Tools.rand_q(n_bits, qsize)
      val naiveRes = Profiler("Naive") {
        dc.naive_eval(query)
      }
      val solverRes = Profiler("Solver") {
        import core.RationalTools._
        var l = dc.m.prepare_online_agg(query, cheap_size)
        val bounds = SolverTools.mk_all_non_neg[Rational](1 << query.length)
        val s = SparseSolver[Rational](query.length, bounds, Nil, Nil, _ => true)
        var df = s.df
        while (!(l.isEmpty) && (df > 0)) {
          val fetched = Profiler("Fetch") {
            dc.fetch2(List(l.head))
          }
          Profiler("SolverAdd") {
            s.add2(List(l.head.accessible_bits), fetched)
          }
          if (df != s.df) {
            Profiler("Gauss") {
              s.gauss(s.det_vars)
            }
            Profiler("ComputeBounds") {
              s.compute_bounds
            }
            df = s.df
          }
          l = l.tail
        }
      }
      println(
        dcloadtime / (1000 * 1000) + "\t" +
          Profiler.durations("Naive")._2 / (1000 * 1000) + "\t" +
          Profiler.durations("Solver")._2 / (1000 * 1000) + "\t" +
          Profiler.durations("Fetch")._2 / (1000 * 1000) + "\t" +
          Profiler.durations("SolverAdd")._2 / (1000 * 1000) + "\t" +
          Profiler.durations("Gauss")._2 / (1000 * 1000) + "\t" +
          Profiler.durations("ComputeBounds")._2 / (1000 * 1000)
      )
    }
    //Profiler.print()
  }

  import util.Profiler
  import scala.util.Random

  def rnd(n: Int, base: Int) = {
    Profiler.resetAll()
    var sum = 0
    Profiler("Random") {
      (0 until (1 << n)).foreach(i => sum += Random.nextInt(1 << base))
    }
    Profiler.print()
    println(sum)
  }

  def func(nbits: Int, lognrows: Int) = {
    val nrows = 1 << lognrows
    val rows = scala.collection.mutable.ArrayBuffer[BigInt]()
    Profiler.resetAll()
    (0 until nrows).foreach { i =>
      val mysum = Profiler("FUNC") {
        (0 until nbits).foldLeft((BigInt(0), BigInt(1))) { case ((sum, prod), cur) =>
          Sampling.f1(2) match {
            case 0 => (sum, prod << 1)
            case 1 => (sum + prod, prod << 1)
          }
        }._1}
        rows += mysum
    }

    Profiler.print()
  }

  def main(args: Array[String]) {
    mkCube(15)
    System.out.flush()
    System.err.flush()
    Profiler.print()


  }
}
