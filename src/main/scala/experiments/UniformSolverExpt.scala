package experiments

import core._
import solver._
import util._
import RationalTools._
import breeze.linalg.DenseVector
import SolverTools._
import core.solver.Strategy.{CoMoment, CoMomentFrechet, Strategy}
import frontend.experiments.Tools

import java.io.PrintStream
import java.time.{Instant, LocalDateTime}
import java.time.format.DateTimeFormatter
import scala.reflect.ClassTag


class UniformSolverExpt[T:Fractional:ClassTag](dc: DataCube, val name: String = "")(implicit shouldRecord: Boolean) {

  val timestamp = if(shouldRecord) {
    val datetime = LocalDateTime.now
    DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").format(datetime)
  } else "dummy"

  val fileout = new PrintStream(s"expdata/UniformSolverExpt_${name}_${timestamp}.csv")
  val strategies = List(Strategy.CoMoment3, CoMomentFrechet) //Strategy.values.toList
  fileout.println("Name,Query, QSize, DOF, NPrepareTime(us), NFetchTime(us), NaiveTotal(us), UPrepareTime(us), UFetchTime(us), SolversTotalTime(us), " + strategies.map(a => s"$a AddTime(us), $a FillTime(us), $a SolveTime(us), $a Err").mkString(", "))
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


  def uniform_solve(q: Seq[Int]) = {

    val l = Profiler("USolve Prepare") {
      dc.m.prepare(q, dc.m.n_bits-1, dc.m.n_bits-1)
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

  def compare(qu: Seq[Int], output: Boolean = true) = {
    val q = qu.sorted
    println(s"\nQuery size = ${q.size} \nQuery = " + qu)
    Profiler.resetAll()
    val naiveRes = Profiler("Naive Full"){dc.naive_eval(q)}
    //val naiveCum = fastMoments(naiveRes)

    val solverRes = Profiler("Solver Full"){uniform_solve(q)}
    val num = implicitly[Fractional[T]]
    //Profiler.print()


    val errors = Profiler("ErrorChecking"){solverRes.map{s => s.strategy ->  error(naiveRes, s.solution)}}
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
      val resultrow = s"${name},${qu.mkString(":")},${q.size},$dof,  $nprepare,$nfetch,$ntotal,  $uprep,$ufetch,$utot,  " +
        errors.map { case (algo, err) =>
          val uadd = Profiler.durations(s"USolve Add $algo")._2 / 1000
          val ufill = Profiler.durations(s"USolve FillMissing $algo")._2 / 1000
          val usolve = Profiler.durations(s"USolve Solve $algo")._2 / 1000
          s"  ${uadd},${ufill},${usolve},${round(err)}"
        }.mkString(", ")
      fileout.println(resultrow)
    }

  }

  def rnd_compare(qsize: Int) = {
    val query = Tools.rand_q(dc.m.n_bits, qsize)
    compare(query)
  }
}

