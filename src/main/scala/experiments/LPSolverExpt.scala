package experiments

import core.{DataCube, RandomizedMaterializationScheme, SolverTools, SparseSolver}
import core.solver.{SliceSparseSolver, Strategy}
import util.Profiler

import java.io.PrintStream
import java.time.Instant
import scala.reflect.ClassTag

class LPSolverExpt[T:Fractional:ClassTag](dc: DataCube, val name: String = "LPSolverExpt") {

  val timestamp = Instant.now().toString

  val lrf = math.log(dc.m.asInstanceOf[RandomizedMaterializationScheme].rf) / math.log(10)
  val lbase = math.log(dc.m.asInstanceOf[RandomizedMaterializationScheme].base) / math.log(10)
  val fileout = new PrintStream(s"expdata/${name}_${timestamp}.csv")

  fileout.println("LogRF,LogBase,Query,QSize,   NPrepareTime(us),NFetchTime(us),NaiveTotal(us), LPPrepareTime(us),LPFetchTime(us),LPTotalTime(us)," +
    "Init SliceSparse, ComputeBounds SliceSparse, DOF SliceSparse, Error SliceSparse,"+
    "Init Sparse, ComputeBounds Sparse, DOF Sparse, Error Sparse"
  )
  println("LP Solver of type " + implicitly[ClassTag[T]])

  def lp_solve(q: Seq[Int]) = {
    val l = Profiler("LPSolve Prepare") {
      dc.m.prepare(q, q.length - 1, q.length - 1)
    }

    val fetched =  Profiler("LPSolve Fetch") {
      dc.fetch2(l)
    }
    val allVars = 0 until (1<<q.size)

    println("\nSliceSparseSolver")
    val s1 = Profiler("Init SliceSparseSolver") {
      val b1 = SolverTools.mk_all_non_neg(1 << q.size)
      new SliceSparseSolver[T](q.length, b1, l.map(_.accessible_bits), fetched)
    }

    Profiler("ComputeBounds SliceSparse") {
      s1.compute_bounds
      s1.propagate_bounds(allVars)
    }

    println("\nSparseSolver")
    val s2 = Profiler("Init SparseSolver") {
      val b2 = SolverTools.mk_all_non_neg(1 << q.size)
      new SparseSolver[T](q.length, b2, l.map(_.accessible_bits), fetched)
    }

    Profiler("ComputeBounds Sparse") {
      s2.compute_bounds
      s2.propagate_bounds(allVars)
    }
    (s1, s2)
  }

  def compare(qu: Seq[Int]) = {
    val q = qu.sorted
    println(s"\nQuery size = ${q.size} \nQuery = " + qu)
    Profiler.resetAll()

    val naiveRes = Profiler("Naive Full"){dc.naive_eval(q)}

    val (s1, s2) = Profiler("Solver Full"){lp_solve(q)}

    val err1 = error(naiveRes, s1)
    val err2 = error(naiveRes, s2)
    val dof1 = s1.df
    val dof2 = s2.df

    println(s"SliceSparseSolver dof = $dof1 error = $err1")
    println(s"SparseSolver dof = $dof2 error = $err2")

    val ntotal = Profiler.durations("Naive Full")._2/1000
    val nprepare = Profiler.durations("NaivePrepare")._2/1000
    val nfetch = Profiler.durations("NaiveFetch")._2/1000
    val lpprep = Profiler.durations("LPSolve Prepare")._2/1000
    val lpfetch = Profiler.durations("LPSolve Fetch")._2/1000
    val lptot = Profiler.durations("Solver Full")._2/1000

    val inits1 = Profiler.durations("Init SliceSparseSolver")._2/1000
    val inits2 = Profiler.durations("Init SparseSolver")._2/1000
    val cbs1 = Profiler.durations("ComputeBounds SliceSparse")._2/1000
    val cbs2 = Profiler.durations("ComputeBounds Sparse")._2/1000


    def round(v: Double) = {
      val prec = 10000
      math.floor(v*prec)/prec
    }


    val resultrow = s"${lrf},${lbase},${qu.mkString(":")},${q.size},  $nprepare,$nfetch,$ntotal,  $lpprep,$lpfetch,$lptot,  " +
      s"$inits1,$cbs1,$dof1,${round(err1)},  " +
      s"$inits2,$cbs2,$dof2,${round(err2)}"
    fileout.println(resultrow)

  }

  def error(naive: Array[Double], solver: SparseSolver[T]) = {
    val num = implicitly[Fractional[T]]
    val span = solver.cumulative_interval_span.map(num.toDouble).getOrElse(Double.PositiveInfinity)
    span/naive.sum
  }
}

