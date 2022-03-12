package experiments

import core._
import solver._
import util._
import RationalTools._
import breeze.linalg.DenseVector
import SolverTools._
import core.solver.Strategy.{CoMoment, CoMomentFrechet, Strategy}
import frontend.experiments.Tools

import java.io.{File, PrintStream}
import java.time.{Instant, LocalDateTime}
import java.time.format.DateTimeFormatter
import scala.reflect.ClassTag


class OldMomentSolverBatchExpt[T:Fractional:ClassTag](val ename2: String = "")(implicit shouldRecord: Boolean) extends Experiment("moment-batch", ename2){

  val strategies = List(Strategy.CoMoment3) //Strategy.values.toList
  fileout.println("Name,Query, QSize, DOF, NPrepareTime(us), NFetchTime(us), NaiveTotal(us),NaiveMaxDimFetched,  SolversTotalTime(us), UPrepareTime(us), UFetchTime(us), USolveMaxDimFetched, " + strategies.map(a => s"$a SolveTime(us), $a Err").mkString(", "))
  //println("Moment Solver of type " + implicitly[ClassTag[T]])

  def uniform_solve(dc: DataCube, q: Seq[Int]) = {

    val l = Profiler("USolve Prepare") {
      dc.m.prepare(q, dc.m.n_bits-1, dc.m.n_bits-1)
    }
    val maxDimFetch = l.last.mask.length
    //println("Solver Prepare Over.  #Cuboids = "+l.size + "  maxDim="+maxDimFetch)
    val fetched =  Profiler("USolve Fetch") {
     l.map { pm =>
       (pm.accessible_bits, dc.fetch2[T](List(pm)).toArray)
      }
    }


    val result = strategies.map { a =>
      Profiler(s"USolve Solve ${a}") {
        val s = Profiler(s"USolve Constructor ${a}") {
          new MomentSolverAll[T](q.length, a)
        }
        Profiler(s"USolve Add ${s.strategy}") {
          fetched.foreach { case (bits, array) => s.add(bits, array) }
        }
        Profiler(s"USolve FillMissing ${s.strategy}") {
          s.fillMissing()
        }
        Profiler(s"USolve FastSolve ${s.strategy}") {
          s.fastSolve()
        }
        s
      }
    }
    (result, maxDimFetch)
  }

  def run(dc: DataCube, dcname:String, qu: Seq[Int], output: Boolean = true) = {
    val q = qu.sorted
    //println(s"\nQuery size = ${q.size} \nQuery = " + qu)
    Profiler.resetAll()
    val (naiveRes, naiveMaxDim) = Profiler("Naive Full"){
      val l = Profiler("NaivePrepare"){dc.m.prepare(q, dc.m.n_bits, dc.m.n_bits)}
      val maxDim = l.head.mask.length
      //println("Naive query "+l.head.mask.sum + "  maxDimFetched = " + maxDim)
      val res = Profiler("NaiveFetch"){dc.fetch(l).map(p => p.sm.toDouble)}
      (res, maxDim)
    }
    //val naiveCum = fastMoments(naiveRes)

    val (solverRes, solverMaxDim) = Profiler("Solver Full"){uniform_solve(dc, q)}
    val num = implicitly[Fractional[T]]
    //Profiler.print()


    val errors = Profiler("ErrorChecking"){solverRes.map{s => s.strategy ->  error(naiveRes, s.solution)}}
    val dof = solverRes.head.dof
    val knownSums = solverRes.head.knownSums

    //println("DOF = " + dof)
    //println("Errors = " + errors.map(_._2).mkString("   "))

    val prec = 1000.0
    //val allcums = naiveCum.indices.map { i =>
    //  val n = (naiveCum(i) * prec/ naiveCum(0)).toInt
    //  val ssv = solverRes.map(s => num.toInt(num.times(num.fromInt(prec.toInt), num.div(s.sumValues(i), s.sumValues(0)))))
    //  (i, n, ssv)
    //}
    //allcums.sortBy{case (i, n, ssv) => Math.abs(ssv.head - n)}.map{case (i, n, ssv) => s"$i ${knownSums(i)} ==> $n     ${ssv.mkString(" ")}"}.foreach(println)

      val ntotal = Profiler.durations("Naive Full")._2/1000
      val nprepare = Profiler.durations("NaivePrepare")._2/1000
      val nfetch = Profiler.durations("NaiveFetch")._2/1000
      val uprep = Profiler.durations("USolve Prepare")._2/1000
      val ufetch = Profiler.durations("USolve Fetch")._2/1000
      val utot = Profiler.durations("Solver Full")._2/1000

      def round(v: Double) = {
        val prec = 10000
        math.floor(v*prec)/prec
      }

    if(output) {
      val resultrow = s"${dcname},${qu.mkString(":")},${q.size},$dof,  $nprepare,$nfetch,$ntotal,$naiveMaxDim,  $utot,$uprep,$ufetch,$solverMaxDim,  " +
        errors.map { case (algo, err) =>
          val usolve = Profiler.durations(s"USolve Solve $algo")._2 / 1000
          s"  ${usolve},${round(err)}"
        }.mkString(", ")
      fileout.println(resultrow)
    }

  }
}

