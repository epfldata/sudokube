package experiments

import core.solver.SolverTools._
import core._
import core.solver._
import util._
import core.solver.moment.Strategy._
import core.prepare.Preparer
import core.solver.moment.{CoMoment3Solver, CoMoment4Solver, Moment1Transformer, MomentSolver}

 class NewMomentSolverBatchExpt(strategy: Strategy, ename2: String = "")(implicit shouldRecord: Boolean) extends Experiment(s"newmoment-batch", ename2) {

  fileout.println("CubeName, SolverName, Query, QSize, DOF, NPrepareTime(us), NFetchTime(us), NaiveTotal(us),NaiveMaxDimFetched,  MTotalTime(us), MPrepareTime(us), MFetchTime(us), MSolveMaxDimFetched, MSolveTime(us), MErr")

  def solver(qsize: Int, pm: Seq[(Int, Double)])(implicit shouldRecord: Boolean): MomentSolver[Double] = strategy match {
    case CoMoment3 => new CoMoment3Solver(qsize, true, Moment1Transformer(), pm)
    case CoMoment4 => new CoMoment4Solver(qsize, true, Moment1Transformer(), pm)
  }

  def moment_solve(dc: DataCube, q: Seq[Int]) = {

    val (l, pm) = Profiler("Moment Prepare") {
      Preparer.default.prepareBatch(dc.m, q, dc.m.n_bits - 1) -> SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
    }
    val maxDimFetch = l.last.mask.length
    //println("Solver Prepare Over.  #Cuboids = "+l.size + "  maxDim="+maxDimFetch)
    val fetched = Profiler("Moment Fetch") {
      l.map { pm =>
        (pm.accessible_bits, dc.fetch2[Double](List(pm)).toArray)
      }
    }
    val result = Profiler(s"Moment Solve") {
      val s = Profiler(s"Moment Constructor") {
        solver(q.length, pm)
      }
      Profiler(s"Moment Add") {
        fetched.foreach { case (bits, array) => s.add(bits, array) }
      }
      Profiler(s"Moment FillMissing") {
        s.fillMissing()
      }
      Profiler(s"Moment ReverseTransform") {
        s.solve()
      }
      s
    }
    (result, maxDimFetch)
  }

  def run(dc: DataCube, dcname: String, qu: Seq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: IndexedSeq[Int]): Unit = {
    import frontend.experiments.Tools.round
    val q = qu.sorted
    //println(s"\nQuery size = ${q.size} \nQuery = " + qu)
    Profiler.resetAll()
    val (naiveRes, naiveMaxDim) = Profiler("Naive Total") {
      val l = Profiler("Naive Prepare") {
        Preparer.default.prepareBatch(dc.m, q, dc.m.n_bits)
      }
      val maxDim = l.head.mask.length
      //println("Naive query "+l.head.mask.sum + "  maxDimFetched = " + maxDim)
      val res = Profiler("Naive Fetch") {
        dc.fetch(l).map(p => p.sm.toDouble)
      }
      (res, maxDim)
    }
    //val naiveMoments = fastMoments(naiveRes)

    val (momentRes, solverMaxDim) = Profiler("Moment Total") {
      moment_solve(dc, q)
    }
    //Profiler.print()
    val error = Profiler("ErrorChecking") {
      SolverTools.error(naiveRes, momentRes.solution)
    }
    val dof = momentRes.dof
    //val moments = momentRes.moments
    //val known = momentRes.knownSet
    //println("DOF = " + dof)
    //println("Error = " + error)

    //val prec = 1000.0
    //val allMoments = naiveMoments.indices.map { i =>
    //  val nm = (naiveMoments(i) * prec / naiveMoments(0)).toInt
    //  val mm = (moments(i) * prec / moments(0)).toInt
    //  (i, nm, mm)
    //}
    //allMoments.sortBy { case (i, nm, mm) => Math.abs(mm - nm) }.map { case (i, nm, mm) => s"$i ${known(i)} ==> $nm  $mm"}.foreach(println)

    val ntotal = Profiler.durations("Naive Total")._2 / 1000
    val nprepare = Profiler.durations("Naive Prepare")._2 / 1000
    val nfetch = Profiler.durations("Naive Fetch")._2 / 1000
    val mprep = Profiler.durations("Moment Prepare")._2 / 1000
    val mfetch = Profiler.durations("Moment Fetch")._2 / 1000
    val mtot = Profiler.durations("Moment Total")._2 / 1000
    val msolve = Profiler.durations(s"Moment Solve")._2 / 1000
    if (output) {
      val resultrow = s"${dcname}, ${momentRes.name}, ${qu.mkString(":")},${q.size},$dof,  $nprepare,$nfetch,$ntotal,$naiveMaxDim,  $mtot,$mprep,$mfetch,$solverMaxDim,$msolve,$error"
      fileout.println(resultrow)
    }

  }
}