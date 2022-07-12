package experiments

import core.prepare.Preparer
import core.solver.lpp.{SliceSparseSolver, SparseSolver}
import core.DataCube
import core.solver.SolverTools
import frontend.experiments.Tools
import util.Profiler

import java.io.{File, PrintStream}
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime}
import scala.reflect.ClassTag

class LPSolverBatchExpt[T: Fractional : ClassTag](val ename2: String = "")(implicit shouldRecord: Boolean) extends Experiment("lp-batch", ename2) {

  fileout.println("CubeName,Query,QSize,   NPrepareTime(us),NFetchTime(us),NaiveTotal(us),NaiveMaxDimFetched, LPPrepareTime(us),LPFetchTime(us),LPTotalTime(us),LPPMaxDim," +
    "Init SliceSparse, ComputeBounds SliceSparse, DOF SliceSparse, Error SliceSparse, " +
    "Init Sparse, ComputeBounds Sparse, DOF Sparse, Error Sparse"
  )
  //println("LP Solver of type " + implicitly[ClassTag[T]])

  def lp_solve(dc: DataCube, q: Seq[Int]) = {
    val l = Profiler("LPSolve Prepare") {
      Preparer.default.prepareBatch(dc.m, q, dc.m.n_bits - 1) //fetch most dominating cuboids other than full
    }
    val prepareMaxDim = l.last.mask.length
    //println("Prepare over. #Cuboids to fetch = "+l.size + "  Last cuboid size =" + prepareMaxDim)

    val fetched = Profiler("LPSolve Fetch") {
      dc.fetch2(l)
    }
    val allVars = 0 until (1 << q.size)

    //println("\nSliceSparseSolver")
    val s1 = Profiler("Init SliceSparseSolver") {
      val b1 = SolverTools.mk_all_non_neg(1 << q.size)
      new SliceSparseSolver[T](q.length, b1, l.map(_.accessible_bits), fetched)
    }

    Profiler("ComputeBounds SliceSparse") {
      s1.compute_bounds
      s1.propagate_bounds(allVars)
    }

    //println("\nSparseSolver")
    //val s2 = Profiler("Init SparseSolver") {
    //  val b2 = SolverTools.mk_all_non_neg(1 << q.size)
    //  new SparseSolver[T](q.length, b2, l.map(_.accessible_bits), fetched)
    //}
    //
    //Profiler("ComputeBounds Sparse") {
    //  s2.compute_bounds
    //  s2.propagate_bounds(allVars)
    //}
    //
    (s1, prepareMaxDim)
  }

  def run(dc: DataCube, dcname: String, qu: Seq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: IndexedSeq[Int]): Unit = {
    val q = qu.sorted
    //println(s"\nQuery size = ${q.size} \nQuery = " + qu)
    Profiler.resetAll()

    val (naiveRes, naiveMaxDim) = Profiler("Naive Full") {
      val l = Profiler("NaivePrepare") {
        Preparer.default.prepareBatch(dc.m, q, dc.m.n_bits)
      }
      val maxDim = l.head.mask.length
      //println("Naive query "+l.head.mask.sum + "  maxDimFetched = " + maxDim)
      val res = Profiler("NaiveFetch") {
        dc.fetch(l).map(p => p.sm.toDouble)
      }
      (res, maxDim)
    }

    val (s1, lpMaxDim) = Profiler("Solver Full") {
      lp_solve(dc, q)
    }

    val err1 = error(naiveRes, s1)
    //val err2 = error(naiveRes, s2)
    val dof1 = s1.df
    //val dof2 = s2.df

    //println(s"SliceSparseSolver dof = $dof1 error = $err1")
    //println(s"SparseSolver dof = $dof2 error = $err2")
    //Profiler.print()
    val ntotal = Profiler.durations("Naive Full")._2 / 1000
    val nprepare = Profiler.durations("NaivePrepare")._2 / 1000
    val nfetch = Profiler.durations("NaiveFetch")._2 / 1000
    val lpprep = Profiler.durations("LPSolve Prepare")._2 / 1000
    val lpfetch = Profiler.durations("LPSolve Fetch")._2 / 1000
    val lptot = Profiler.durations("Solver Full")._2 / 1000

    val inits1 = Profiler.durations("Init SliceSparseSolver")._2 / 1000
    //val inits2 = Profiler.durations("Init SparseSolver")._2/1000
    val cbs1 = Profiler.durations("ComputeBounds SliceSparse")._2 / 1000
    //val cbs2 = Profiler.durations("ComputeBounds Sparse")._2/1000



    if (output) {
      val resultrow = s"$dcname,${qu.mkString(":")},${q.size},  $nprepare,$nfetch,$ntotal,$naiveMaxDim,  $lpprep,$lpfetch,$lptot,$lpMaxDim,  " +
        s"$inits1,$cbs1,$dof1,${Tools.round(err1, 4)}  "
      //s"$inits2,$cbs2,$dof2,${round(err2)}"
      fileout.println(resultrow)
    }

  }

  def error(naive: Array[Double], solver: SparseSolver[T]) = {
    val num = implicitly[Fractional[T]]
    val span = solver.cumulative_interval_span.map(num.toDouble).getOrElse(Double.PositiveInfinity)
    span / naive.sum
  }
}

