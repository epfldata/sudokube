package experiments

import core.SolverTools.error
import core.{DataCube, SolverTools}
import core.solver.{CoMoment4Solver, Moment1Transformer}
import core.solver.iterativeProportionalFittingSolver.VanillaIPFSolver
import util.Profiler

import java.io.{File, PrintStream}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class VanillaIPFMomentBatchExpt(ename2: String = "")(implicit shouldRecord: Boolean) extends Experiment(s"vanilla-ipf-moment-batch", ename2) {
  fileout.println(
    "CubeName, MomentSolverName, Query, QSize, DOF, " +
      "NPrepareTime(us), NFetchTime(us), NaiveTotal(us),NaiveMaxDimFetched,NaiveEntropy,  " +
      "MTotalTime(us), MPrepareTime(us), MFetchTime(us), MSolveMaxDimFetched, MSolveTime(us), MErr, MEntropy, " +
      "IPFTotalTime(us), IPFPrepareTime(us), IPFFetchTime(us), IPFMaxDimFetched, IPFSolveTime(us), IPFErr, IPFEntropy, " +
      "Difference,MaxDifference"
  )

  val ipfTimeErrorFileout: PrintStream = {
    val isFinal = true
    val (timestamp, folder) = {
      if (isFinal) ("final", ".")
      else if (shouldRecord) {
        val datetime = LocalDateTime.now
        (DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").format(datetime), DateTimeFormatter.ofPattern("yyyyMMdd").format(datetime))
      } else ("dummy", "dummy")
    }
    val file = new File(s"expdata/$folder/${ename2}_vanilla-ipf-time-error_$timestamp.csv")
    if (!file.exists())
      file.getParentFile.mkdirs()
    new PrintStream(file)
  }

  ipfTimeErrorFileout.println("CubeName, Query, QSize, IPFTotalTime(us), IPFSolveTime(us), IPFErr")

  def moment_solve(dc: DataCube, q: Seq[Int]): (CoMoment4Solver, Int) = {
    val (l, pm) = Profiler("Moment Prepare") {
      dc.m.prepare(q, dc.m.n_bits - 1, dc.m.n_bits - 1) -> SolverTools.preparePrimaryMomentsForQuery(q, dc.primaryMoments)
    }
    val maxDimFetch = l.last.mask.length
    val fetched = Profiler("Moment Fetch") {
      l.map { pm =>
        (pm.accessible_bits, dc.fetch2[Double](List(pm)).toArray)
      }
    }

    val result = Profiler("Moment Solve") {
      val s = Profiler("Moment Constructor") {
        new CoMoment4Solver(q.length, true, Moment1Transformer, pm)
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

  def ipf_solve(dc: DataCube, q: Seq[Int], naiveRes: Array[Double], dcname: String, qu: Seq[Int]): (VanillaIPFSolver, Int) = {
    val (l, _) = Profiler("Vanilla IPF Prepare") { // Same as moment for the moment
      dc.m.prepare(q, dc.m.n_bits - 1, dc.m.n_bits - 1) -> SolverTools.preparePrimaryMomentsForQuery(q, dc.primaryMoments)
    }
    val maxDimFetch = l.last.mask.length
    val fetched = Profiler("Vanilla IPF Fetch") { // Same as moment for the moment
      l.map { pm =>
        (pm.accessible_bits, dc.fetch2[Double](List(pm)).toArray)
      }
    }

    println(s"\t\tNumber of cubes fetched: ${fetched.length}")

    val result = Profiler("Vanilla IPF Solve") {
      val solver = Profiler("Vanilla IPF Constructor") {
        new VanillaIPFSolver(q.length, true, naiveRes, ipfTimeErrorFileout, dcname, qu.mkString(":"))
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

  def run(dc: DataCube, dcname: String, qu: Seq[Int], output: Boolean = true, qname: String = ""): Unit = {
    val q = qu.sorted

    Profiler.resetAll()

    val (naiveRes, naiveMaxDim) = Profiler("Naive Total") {
      val l = Profiler("Naive Prepare") {
        dc.m.prepare(q, dc.m.n_bits, dc.m.n_bits)
      }
      val maxDim = l.head.mask.length
      val res = Profiler("Naive Fetch") {
        dc.fetch(l).map(_.sm)
      }
      (res, maxDim)
    }
    println("\t\tNaive total time: " + Profiler.durations("Naive Total")._2 / 1000)


    val (momentSolver, momentMaxDim) = Profiler("Moment Total") {
      moment_solve(dc, q)
    }
    val momentError = Profiler("Moment Error Checking") {
      SolverTools.error(naiveRes, momentSolver.solution)
    }
    val dof = momentSolver.dof
    println("\t\tMoment solve time: " + Profiler.durations("Moment Solve")._2 / 1000 +
            ", total time: " + Profiler.durations("Moment Total")._2 / 1000 +
            ", error: " + momentError)


    val (vanillaIPFSolver, ipfMaxDim) = Profiler("Vanilla IPF Total") {
      ipf_solve(dc, q, naiveRes, dcname, qu)
    }
    val ipfError = Profiler("Vanilla IPF Error Checking") {
      SolverTools.error(naiveRes, vanillaIPFSolver.getSolution)
    }
    println("\t\tVanilla IPF solve time: " + Profiler.durations("Vanilla IPF Solve")._2 / 1000 +
            ", total time: " + Profiler.durations("Vanilla IPF Total")._2 / 1000 +
            ", error: " + ipfError)


    val difference = error(momentSolver.solution, vanillaIPFSolver.getSolution)
    println(s"\t\tDifference (using error measure) = $difference")

    val grandTotal = naiveRes.sum
    val maxDifference = (0 until 1 << q.length).map(i => (momentSolver.solution(i) - vanillaIPFSolver.getSolution(i)).abs).max / grandTotal
    println(s"\t\tMax difference out of total sum = $maxDifference")


    val naiveEntropy = entropy(naiveRes)
    val momentEntropy = entropy(momentSolver.solution)
    val vanillaIPFEntropy = entropy(vanillaIPFSolver.getSolution)
    println(s"\t\tNaive (real) Entropy = $naiveEntropy")
    println(s"\t\tMoment Entropy = $momentEntropy")
    println(s"\t\tVanilla IPF Entropy = $vanillaIPFEntropy")

    val ntotal = Profiler.durations("Naive Total")._2 / 1000
    val nprepare = Profiler.durations("Naive Prepare")._2 / 1000
    val nfetch = Profiler.durations("Naive Fetch")._2 / 1000

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
        s"$nprepare,$nfetch,$ntotal,$naiveMaxDim,$naiveEntropy,  " +
        s"$mtot,$mprep,$mfetch,$momentMaxDim,$msolve,$momentError,$momentEntropy, " +
        s"$ipfTotal,$ipfPrepare,$ipfFetch,$ipfMaxDim,$ipfSolve,$ipfError,$vanillaIPFEntropy, " +
        s"$difference,$maxDifference"
      fileout.println(resultrow)
    }

  }

  def entropy(p: Array[Double]): Double = {
    val grandTotal = p.sum
    p.map(n => if (n != 0) - (n/grandTotal) * math.log(n/grandTotal) else 0).sum
  }
}
