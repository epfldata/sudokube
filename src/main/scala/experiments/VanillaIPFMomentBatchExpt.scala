package experiments

import core.SolverTools.error
import core.{DataCube, SolverTools}
import core.solver.{CoMoment3Solver, CoMoment4Solver, Moment1Transformer}
import core.solver.iterativeProportionalFittingSolver.VanillaIPFSolver
import util.Profiler

class VanillaIPFMomentBatchExpt(ename2: String = "")(implicit shouldRecord: Boolean) extends Experiment(s"vanilla-ipf-moment-batch", ename2) {
  fileout.println(
    "CubeName, SolverName, Query, QSize, DOF, " +
      "MTotalTime(us), MPrepareTime(us), MFetchTime(us), MSolveMaxDimFetched, MSolveTime(us), MErr, " +
      "IPFTotalTime(us), IPFPrepareTime(us), IPFFetchTime(us), IPFMaxDimFetched, IPFSolveTime(us), IPFErr"
  )

  def moment_solve(dc: DataCube, q: Seq[Int]): (CoMoment4Solver[Double], Int) = {
    val (l, pm) = Profiler("Moment Prepare") {
      dc.m.prepare(q, dc.m.n_bits - 1, dc.m.n_bits - 1) -> SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
    }
    val maxDimFetch = l.last.mask.length
    val fetched = Profiler("Moment Fetch") {
      l.map { pm =>
        (pm.accessible_bits, dc.fetch2[Double](List(pm)).toArray)
      }
    }

    val result = Profiler("Moment Solve") {
      val s = Profiler("Moment Constructor") {
        new CoMoment4Solver[Double](q.length, true, Moment1Transformer(), pm)
      }
      Profiler("Moment Add") {
        fetched.foreach { case (bits, array) => s.add(bits, array) }
      }
      Profiler("Moment FillMissing") {
        s.fillMissing()
      }
      Profiler("Moment ReverseTransform") {
        s.solve()
      }
      s
    }
    (result, maxDimFetch)
  }

  def ipf_solve(dc: DataCube, q: Seq[Int]): (VanillaIPFSolver, Int) = {
    val (l, _) = Profiler("Vanilla IPF Prepare") { // Same as moment for the moment
      dc.m.prepare(q, dc.m.n_bits - 1, dc.m.n_bits - 1) -> SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
    }
    val maxDimFetch = l.last.mask.length
    val fetched = Profiler("Vanilla IPF Fetch") { // Same as moment for the moment
      l.map { pm =>
        (pm.accessible_bits, dc.fetch2[Double](List(pm)).toArray)
      }
    }

    val result = Profiler("Vanilla IPF Solve") {
      val solver = Profiler("Vanilla IPF Constructor") {
        new VanillaIPFSolver(q.length)
      }
      Profiler("Vanilla IPF Add") {
        fetched.foreach { case (bits, array) => solver.add(bits, array) }
      }
      Profiler("Vanilla IPF Iterations") {
        solver.solve()
      }
      solver
    }
    (result, maxDimFetch)
  }

  def run(dc: DataCube, dcname: String, qu: Seq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = ""): Unit = {
    val q = qu.sorted

    Profiler.resetAll()

    val (momentSolver, momentMaxDim) = Profiler("Moment Total") {
      moment_solve(dc, q)
    }
    val momentError = Profiler("Moment Error Checking") {
      SolverTools.error(trueResult, momentSolver.solution)
    }
    val dof = momentSolver.dof
    println("\t\tMoment solve time: " + Profiler.durations("Moment Solve")._2 / 1000 +
            ", total time: " + Profiler.durations("Moment Total")._2 / 1000 +
            ", error: " + momentError)


    val (vanillaIPFSolver, ipfMaxDim) = Profiler("Vanilla IPF Total") {
      ipf_solve(dc, q)
    }
    val ipfError = Profiler("Vanilla IPF Error Checking") {
      SolverTools.error(trueResult, vanillaIPFSolver.totalDistribution)
    }
    println("\t\tVanilla IPF solve time: " + Profiler.durations("Vanilla IPF Solve")._2 / 1000 +
            ", total time: " + Profiler.durations("Vanilla IPF Total")._2 / 1000 +
            ", error: " + ipfError)

    println("\t\tDifference (using error measure) = " + error(momentSolver.solution, vanillaIPFSolver.totalDistribution))

    val grandTotal = trueResult.sum
    println("\t\tMax difference out of total sum = " +
      (0 until 1 << q.length).map(i => (momentSolver.solution(i) - vanillaIPFSolver.totalDistribution(i)).abs).max / grandTotal
    )

    println("\t\tMoment Entropy = " + momentSolver.solution.map(n => if (n != 0) - (n/grandTotal) * math.log(n/grandTotal) else 0).sum )
    println("\t\tVanilla IPF Entropy = " + vanillaIPFSolver.totalDistribution.map(n => if (n != 0) - (n/grandTotal) * math.log(n/grandTotal) else 0).sum )

    Profiler.print()

    val mprep = Profiler.durations("Moment Prepare")._2 / 1000
    val mfetch = Profiler.durations("Moment Fetch")._2 / 1000
    val mtot = Profiler.durations("Moment Total")._2 / 1000
    val msolve = Profiler.durations("Moment Solve")._2 / 1000

    val ipfPrepare = Profiler.durations("Vanilla IPF Prepare")._2 / 1000
    val ipfFetch = Profiler.durations("Vanilla IPF Fetch")._2 / 1000
    val ipfSolve = Profiler.durations("Vanilla IPF Solve")._2 / 1000
    val ipfTotal = Profiler.durations("Vanilla IPF Total")._2 / 1000

    if (output) {
      val resultrow = s"$dcname, ${momentSolver.name}, ${qu.mkString(":")},${q.size},$dof,  " +
        s"$mtot,$mprep,$mfetch,$momentMaxDim,$msolve,$momentError, " +
        s"$ipfTotal,$ipfPrepare,$ipfFetch,$ipfMaxDim,$ipfSolve,$ipfError"
      fileout.println(resultrow)
    }

  }
}
