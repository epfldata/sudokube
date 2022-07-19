package experiments

import core.DataCube
import core.solver.SolverTools
import core.solver.SolverTools.error
import core.solver.iterativeProportionalFittingSolver._
import core.solver.moment.{CoMoment4Solver, Moment1Transformer}
import util.{Bits, Profiler}

import java.io.{File, PrintStream}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * TODO: add data by new methods of junction trees & graphs to fileout when finalized and running real experiments
 */
class IPFMomentBatchExpt(ename2: String = "")(implicit shouldRecord: Boolean) extends Experiment(s"vanilla-ipf-moment-batch", ename2) {
  fileout.println(
    "CubeName, MomentSolverName, Query, QSize, NCubesFetched, CubeSizes, DOF, trueEntropy " +
      "MTotalTime(us), MPrepareTime(us), MFetchTime(us), MSolveMaxDimFetched, MSolveTime(us), MErr, MEntropy, " +
      "VIPFTotalTime(us), VIPFPrepareTime(us), VIPFFetchTime(us), VIPFMaxDimFetched, VIPFSolveTime(us), VIPFErr, VIPFEntropy, " +
      "EIPFTotalTime(us), EIPFPrepareTime(us), EIPFFetchTime(us), EIPFMaxDimFetched, EIPFSolveTime(us), EIPFErr, EIPFEntropy, " +
      "LIPFTotalTime(us), LIPFPrepareTime(us), LIPFFetchTime(us), LIPFMaxDimFetched, LIPFSolveTime(us), LIPFErr, LIPFEntropy, " +
      "Difference_vanillaIPF_vs_moment,MaxDifference_vanillaIPF_vs_moment, " +
      "Difference_effectiveIPF_vs_vanillaIPF,MaxDifference_effectiveIPF_vs_vanillaIPF, " +
      "Difference_loopyIPF_vs_vanillaIPF,MaxDifference_loopyIPF_vs_vanillaIPF"
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

  /**
   * Same as the experimentation for moment solver.
   * @param dc DataCube object.
   * @param q Sequence of queried dimensions.
   * @return Tuple consisting of the result and the maximum dimension fetched.
   */
  def moment_solve(dc: DataCube, q: IndexedSeq[Int]): (CoMoment4Solver[Double], Int) = {
    val (l, pm) = Profiler("Moment Prepare") {
      dc.index.prepareBatch(q) -> SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
    }
    val maxDimFetch = l.last.cuboidCost
    val fetched = Profiler("Moment Fetch") {
      l.map { pm =>
        (pm.queryIntersection, dc.fetch2[Double](List(pm)).toArray)
      }
    }

    val result = Profiler("Moment Solve") {
      val s = Profiler("Moment Constructor") {
        new CoMoment4Solver(q.length, true, Moment1Transformer[Double](), pm)
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

  /**
   * Solving with the vanilla version of IPF.
   * @param dc DataCube object.
   * @param q Sequence of queried dimensions.
   * @param trueRes True result of the query (optional, only used if we want to observe errors while experimenting).
   * @param dcname Name of the data cube (optional, only used if we want to see it while experimenting).
   * @return Query result, maximum dimension fetched, number of cuboids fetched, sizes of fetched cuboids.
   */
  def vanillaIPF_solve(dc: DataCube, q: IndexedSeq[Int], trueRes: Array[Double], dcname: String): (VanillaIPFSolver, Int, Int, Seq[Int]) = {
    val l = Profiler("Vanilla IPF Prepare") { // Not doing prepare for primary moments
      dc.index.prepareBatch(q)
    }
    val maxDimFetch = l.last.cuboidCost
    val fetched = Profiler("Vanilla IPF Fetch") { // Same as moment for now
      l.map { pm =>
        (pm.queryIntersection, dc.fetch2[Double](List(pm)))
      }
    }

    println(s"\t\t\tNumber of cubes fetched: ${fetched.length}, Cube sizes: ${ fetched.map{ case (bits, _) => Bits.sizeOfSet(bits) }.mkString(":")}")

    val result = Profiler("Vanilla IPF Constructor + Add + Solve") {
      val solver = Profiler("Vanilla IPF Constructor") {
        new VanillaIPFSolver(q.length, true, trueRes, ipfTimeErrorFileout, dcname, q.mkString(":"))
      }
      Profiler("Vanilla IPF Add") {
        fetched.foreach { case (bits, array) => solver.add(bits, array) }
      }
      Profiler("Vanilla IPF Solve") {
        solver.solve()
      }
      solver
    }
    (result, maxDimFetch, fetched.length, fetched.map { case (bits, _) => Bits.sizeOfSet(bits) })
  }

  /**
   * Solving with the effective version of IPF (with junction tree).
   * @param dc DataCube object.
   * @param q Sequence of queried dimensions.
   * @param trueRes True result of the query (optional, only used if we want to observe errors while experimenting).
   * @param dcname Name of the data cube (optional, only used if we want to see it while experimenting).
   * @return Query result, maximum dimension fetched, number of cuboids fetched, sizes of fetched cuboids.
   */
  def effectiveIPF_solve(dc: DataCube, q: IndexedSeq[Int], trueRes: Array[Double], dcname: String): (EffectiveIPFSolver, Int, Int, Seq[Int]) = {
    val l = Profiler("Effective IPF Prepare") { // Not doing prepare for primary moments
      dc.index.prepareBatch(q)
    }
    val maxDimFetch = l.last.cuboidCost
    val fetched = Profiler("Effective IPF Fetch") { // Same as moment for now

      l.map { pm =>
        (pm.queryIntersection, dc.fetch2[Double](List(pm)))
      }
    }

    val result = Profiler("Effective IPF Constructor + Add + Solve") {
      val solver = Profiler("Effective IPF Constructor") {
        new EffectiveIPFSolver(q.length)
      }
      Profiler("Effective IPF Add") {
        fetched.foreach { case (bits, array) => solver.add(bits, array) }
      }
      Profiler("Effective IPF Solve") {
        solver.solve(/*fetched*/)
      }
      solver
    }
    (result, maxDimFetch, fetched.length, fetched.map { case (bits, _) => Bits.sizeOfSet(bits) })
  }

  /**
   * Solving with the worst loopy IPF (each cluster is in a distinct clique).
   * @param dc DataCube object.
   * @param q Sequence of queried dimensions.
   * @param trueRes True result of the query (optional, only used if we want to observe errors while experimenting).
   * @param dcname Name of the data cube (optional, only used if we want to see it while experimenting).
   * @return Query result, maximum dimension fetched, number of cuboids fetched, sizes of fetched cuboids.
   */
  def loopyIPF_solve(dc: DataCube, q: IndexedSeq[Int], trueRes: Array[Double], dcname: String): (LoopyIPFSolver, Int, Int, Seq[Int]) = {
    val l = Profiler("Loopy IPF Prepare") { // Not doing prepare for primary moments
      dc.index.prepareBatch(q)
    }
    val maxDimFetch = l.last.cuboidCost
    val fetched = Profiler("Loopy IPF Fetch") { // Same as moment for now

      l.map { pm =>
        (pm.queryIntersection, dc.fetch2[Double](List(pm)))
      }
    }

    val result = Profiler("Loopy IPF Constructor + Add + Solve") {
      val solver = Profiler("Loopy IPF Constructor") {
        new LoopyIPFSolver(q.length)
      }
      Profiler("Loopy IPF Add") {
        fetched.foreach { case (bits, array) => solver.add(bits, array) }
      }
      Profiler("Loopy IPF Solve") {
        solver.solve(/*fetched*/)
      }
      solver
    }
    (result, maxDimFetch, fetched.length, fetched.map { case (bits, _) => Bits.sizeOfSet(bits) })
  }

  /**
   * Solving with a junction graph where each cluster is placed randomly in one of the cliques that contains at least one intersecting variable.
   * @param dc DataCube object.
   * @param q Sequence of queried dimensions.
   * @param trueRes True result of the query (optional, only used if we want to observe errors while experimenting).
   * @param dcname Name of the data cube (optional, only used if we want to see it while experimenting).
   * @return Query result, maximum dimension fetched, number of cuboids fetched, sizes of fetched cuboids.
   */
  def randomJunctionGraphIPF_solve(dc: DataCube, q: IndexedSeq[Int], trueRes: Array[Double], dcname: String): (LoopyIPFSolver, Int, Int, Seq[Int]) = {
    val l = Profiler("Random Junction Graph IPF Prepare") { // Not doing prepare for primary moments
      dc.index.prepareBatch(q)
    }
    val maxDimFetch = l.last.cuboidCost
    val fetched = Profiler("Random Junction Graph IPF Fetch") { // Same as moment for now

      l.map { pm =>
        (pm.queryIntersection, dc.fetch2[Double](List(pm)))
      }
    }

    val result = Profiler("Random Junction Graph IPF Constructor + Add + Solve") {
      val solver = Profiler("Random Junction Graph IPF Constructor") {
        new RandomJunctionGraphIPFSolver(q.length)
      }
      Profiler("Random Junction Graph IPF Add") {
        fetched.foreach { case (bits, array) => solver.add(bits, array) }
      }
      Profiler("Random Junction Graph IPF Solve") {
         solver.solve(/*fetched*/)
      }
      solver
    }
    (result, maxDimFetch, fetched.length, fetched.map { case (bits, _) => Bits.sizeOfSet(bits)})
  }

  /**
   * Solving with a junction tree where some low dimensional cuboids are thrown out.
   * @param dc DataCube object.
   * @param q Sequence of queried dimensions.
   * @param trueRes True result of the query (optional, only used if we want to observe errors while experimenting).
   * @param dcname Name of the data cube (optional, only used if we want to see it while experimenting).
   * @return Query result, maximum dimension fetched, number of cuboids fetched, sizes of fetched cuboids.
   */
  def dropoutEffectiveIPF_solve(dc: DataCube, q: IndexedSeq[Int], trueRes: Array[Double], dcname: String): (EffectiveIPFSolver, Int, Int, Seq[Int]) = {
    val l = Profiler("Dropout Effective IPF Prepare") { // Not doing prepare for primary moments
      dc.index.prepareBatch(q)
    }
    val maxDimFetch = l.last.cuboidCost
    val fetched = Profiler("Dropout Effective IPF Fetch") { // Same as moment for now

      l.map { pm =>
        (pm.queryIntersection, dc.fetch2[Double](List(pm)))
      }
    }

    val result = Profiler("Dropout Effective IPF Constructor + Add + Solve") {
      val solver = Profiler("Dropout Effective IPF Constructor") {
        new DropoutEffectiveIPFSolver(q.length)
      }
      Profiler("Dropout Effective IPF Add") {
        fetched.foreach { case (bits, array) => solver.add(bits, array) }
      }
      Profiler("Dropout Effective IPF Solve") {
        solver.solve(/*fetched*/)
      }
      solver
    }
    (result, maxDimFetch, fetched.length, fetched.map { case (bits, _) => Bits.sizeOfSet(bits) })
  }

  def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: IndexedSeq[Int]): Unit = {
    val q = qu.sorted

    Profiler.resetAll()



    println("\t\tMoment solver starts")

    val (momentSolver, momentMaxDim) = Profiler("Moment Total") {
      moment_solve(dc, q)
    }
    val momentError = Profiler("Moment Error Checking") {
      SolverTools.error(trueResult, momentSolver.solution)
    }
    val dof = momentSolver.dof
    println("\t\t\tMoment solve time: " + Profiler.durations("Moment Solve")._2 / 1000 +
            ", total time: " + Profiler.durations("Moment Total")._2 / 1000 +
            ", error: " + momentError)



    println("\t\tVanilla IPF starts")

    val (vanillaIPFSolver, vanillaIPFMaxDim, vanillaIpfNumCubesFetched, vanillaIPFCubeSizes) = Profiler("Vanilla IPF Total") {
      vanillaIPF_solve(dc, q, trueResult, dcname)
    }
    val vanillaIPFError = Profiler("Vanilla IPF Error Checking") {
      SolverTools.error(trueResult, vanillaIPFSolver.getSolution)
    }

    println("\t\t\tVanilla IPF solve time: " + Profiler.durations("Vanilla IPF Solve")._2 / 1000 +
            ", total time: " + Profiler.durations("Vanilla IPF Total")._2 / 1000 +
            ", error: " + vanillaIPFError)




    println("\t\tEffective IPF starts")

    val (effectiveIPFSolver, effectiveIPFMaxDim, effectiveIPFNumCubesFetched, effectiveIPFCubeSizes) = Profiler("Effective IPF Total") {
      effectiveIPF_solve(dc, q, trueResult, dcname)
    }
    val effectiveIPFError = Profiler("Effective IPF Error Checking") {
      effectiveIPFSolver.getTotalDistribution
      SolverTools.error(trueResult, effectiveIPFSolver.getSolution)
    }


    println("\t\t\tEffective IPF solve time: " + Profiler.durations("Effective IPF Solve")._2 / 1000 +
      ", total time: " + Profiler.durations("Effective IPF Total")._2 / 1000 +
      ", error: " + effectiveIPFError)
    println(s"\t\t\tEffective IPF junction tree construction time: ${Profiler.durations("Effective IPF Junction Tree Construction")._2 / 1000}, "
      + s"iterations time: ${Profiler.durations("Effective IPF Iterations")._2 / 1000}, "
      + s"solution derivation time: ${Profiler.durations("Effective IPF Solution Derivation")._2 / 1000}")



    println("\t\tDropout Effective IPF starts")

    val (dropoutEffectiveIPFSolver, dropOutEffectiveIPFMaxDim, dropoutEffectiveIPFNumCubesFetched, dropoutEffectiveIPFCubeSizes) = Profiler("Dropout Effective IPF Total") {
      dropoutEffectiveIPF_solve(dc, q, trueResult, dcname)
    }
    val dropoutEffectiveIPFError = Profiler("Dropout Effective IPF Error Checking") {
      dropoutEffectiveIPFSolver.getTotalDistribution
      SolverTools.error(trueResult, dropoutEffectiveIPFSolver.getSolution)
    }


    println("\t\t\tDropout Effective IPF solve time: " + Profiler.durations("Dropout Effective IPF Solve")._2 / 1000 +
      ", total time: " + Profiler.durations("Dropout Effective IPF Total")._2 / 1000 +
      ", error: " + dropoutEffectiveIPFError)
    println(s"\t\t\tDropout Effective IPF junction tree construction time: ${Profiler.durations("Dropout Effective IPF Junction Tree Construction")._2 / 1000}, "
      + s"iterations time: ${Profiler.durations("Dropout Effective IPF Iterations")._2 / 1000}, "
      + s"solution derivation time: ${Profiler.durations("Dropout Effective IPF Solution Derivation")._2 / 1000}")



    println("\t\tLoopy IPF starts")

    val (loopyIPFSolver, loopyIPFMaxDim, loopyIPFNumCubesFetched, loopyIPFCubeSizes) = Profiler("Loopy IPF Total") {
      loopyIPF_solve(dc, q, trueResult, dcname)
    }
    val loopyIPFError = Profiler("Loopy IPF Error Checking") {
      loopyIPFSolver.getTotalDistribution
      SolverTools.error(trueResult, loopyIPFSolver.getSolution)
    }


    println("\t\t\tLoopy IPF solve time: " + Profiler.durations("Loopy IPF Solve")._2 / 1000 +
      ", total time: " + Profiler.durations("Loopy IPF Total")._2 / 1000 +
      ", error: " + loopyIPFError)
    println(s"\t\t\tLoopy IPF junction graph construction time: ${Profiler.durations("Loopy IPF Junction Graph Construction")._2 / 1000}, "
      + s"iterations time: ${Profiler.durations("Loopy IPF Iterations")._2 / 1000}, "
      + s"solution derivation time: ${Profiler.durations("Loopy IPF Solution Derivation")._2 / 1000}")




    println("\t\tRandom Junction Graph IPF starts")

    val (randomJunctionGraphIPFSolver, randomJunctionGraphIPFMaxDim, randomJunctionGraphIPFNumCubesFetched, randomJunctionGraphIPFCubeSizes) =
      Profiler("Random Junction Graph IPF Total") { randomJunctionGraphIPF_solve(dc, q, trueResult, dcname) }
    val randomJunctionGraphIPFError = Profiler("Random Junction Graph IPF Error Checking") {
       randomJunctionGraphIPFSolver.getTotalDistribution
       SolverTools.error(trueResult, randomJunctionGraphIPFSolver.getSolution)
    }


    println("\t\t\tRandom Junction Graph IPF solve time: " + Profiler.durations("Random Junction Graph IPF Solve")._2 / 1000 +
      ", total time: " + Profiler.durations("Random Junction Graph IPF Total")._2 / 1000 +
      ", error: " + randomJunctionGraphIPFError)
    println(s"\t\t\tRandom Junction Graph IPF junction graph construction time: ${Profiler.durations("Random Junction Graph IPF Junction Graph Construction")._2 / 1000}, "
      + s"iterations time: ${Profiler.durations("Random Junction Graph IPF Iterations")._2 / 1000}, "
      + s"solution derivation time: ${Profiler.durations("Random Junction Graph IPF Solution Derivation")._2 / 1000}")




    println("\t\tDifferences")

    val difference_vanillaIPF_moment = error(momentSolver.solution, vanillaIPFSolver.getSolution)
    print(s"\t\t\tVanilla IPF vs moment: $difference_vanillaIPF_moment, ")

    val grandTotal = trueResult.sum
    vanillaIPFSolver.getSolution
    val maxDifference_vanillaIPF_moment = (0 until 1 << q.length).map(i => (momentSolver.solution(i) - vanillaIPFSolver.solution(i)).abs).max / grandTotal
    println(s"max difference out of total sum: $maxDifference_vanillaIPF_moment")

    val difference_effectiveIPF_vanillaIPF = error(vanillaIPFSolver.getSolution, effectiveIPFSolver.getSolution)
    print(s"\t\t\tEffective IPF vs vanilla IPF: $difference_effectiveIPF_vanillaIPF ")

    val maxDifference_effectiveIPF_vanillaIPF = (0 until 1 << q.length).map(i => (vanillaIPFSolver.solution(i) - effectiveIPFSolver.solution(i)).abs).max / grandTotal
    println(s"Max difference out of total sum: $maxDifference_effectiveIPF_vanillaIPF")

    val difference_loopyIPF_vanillaIPF = error(vanillaIPFSolver.getSolution, loopyIPFSolver.getSolution)
    print(s"\t\t\tLoopy IPF vs vanilla IPF: $difference_loopyIPF_vanillaIPF ")

    val maxDifference_loopyIPF_vanillaIPF = (0 until 1 << q.length).map(i => (vanillaIPFSolver.solution(i) - loopyIPFSolver.solution(i)).abs).max / grandTotal
    println(s"Max difference out of total sum: $maxDifference_loopyIPF_vanillaIPF")


    val trueEntropy = entropy(trueResult)
    val momentEntropy = entropy(momentSolver.solution)
    val vanillaIPFEntropy = entropy(vanillaIPFSolver.getSolution)
    val effectiveIPFEntropy = entropy(effectiveIPFSolver.getSolution)
    val loopyIPFEntropy = entropy(loopyIPFSolver.getSolution)
    println("\t\tEntropies")
    println(s"\t\t\tTrue Entropy = $trueEntropy")
    println(s"\t\t\tMoment Entropy = $momentEntropy")
    println(s"\t\t\tVanilla IPF Entropy = $vanillaIPFEntropy")
    println(s"\t\t\tEffective IPF Entropy = $effectiveIPFEntropy")
    println(s"\t\t\tLoopy IPF Entropy = $loopyIPFEntropy")

    val mprep = Profiler.durations("Moment Prepare")._2 / 1000
    val mfetch = Profiler.durations("Moment Fetch")._2 / 1000
    val mtot = Profiler.durations("Moment Total")._2 / 1000
    val msolve = Profiler.durations("Moment Solve")._2 / 1000

    val vanillaIPFPrepare = Profiler.durations("Vanilla IPF Prepare")._2 / 1000
    val vanillaIPFFetch = Profiler.durations("Vanilla IPF Fetch")._2 / 1000
    val vanillaIPFSolve = Profiler.durations("Vanilla IPF Solve")._2 / 1000
    val vanillaIPFTotal = Profiler.durations("Vanilla IPF Total")._2 / 1000

    val effectiveIPFPrepare = Profiler.durations("Effective IPF Prepare")._2 / 1000
    val effectiveIPFFetch = Profiler.durations("Effective IPF Fetch")._2 / 1000
    val effectiveIPFSolve = Profiler.durations("Effective IPF Solve")._2 / 1000
    val effectiveIPFTotal = Profiler.durations("Effective IPF Total")._2 / 1000

    val loopyIPFPrepare = Profiler.durations("Loopy IPF Prepare")._2 / 1000
    val loopyIPFFetch = Profiler.durations("Loopy IPF Fetch")._2 / 1000
    val loopyIPFSolve = Profiler.durations("Loopy IPF Solve")._2 / 1000
    val loopyIPFTotal = Profiler.durations("Loopy IPF Total")._2 / 1000

    if (output) {
      val resultrow = s"$dcname, ${momentSolver.name}, ${qu.mkString(":")},${q.size},$vanillaIpfNumCubesFetched,${vanillaIPFCubeSizes.mkString(":")},$dof,  " +
        s"$trueEntropy,  " +
        s"$mtot,$mprep,$mfetch,$momentMaxDim,$msolve,$momentError,$momentEntropy, " +
        s"$vanillaIPFTotal,$vanillaIPFPrepare,$vanillaIPFFetch,$vanillaIPFMaxDim,$vanillaIPFSolve,$vanillaIPFError,$vanillaIPFEntropy, " +
        s"$effectiveIPFTotal,$effectiveIPFPrepare,$effectiveIPFFetch,$effectiveIPFMaxDim,$effectiveIPFSolve,$effectiveIPFError,$effectiveIPFEntropy, " +
        s"$loopyIPFTotal,$loopyIPFPrepare,$loopyIPFFetch,$loopyIPFMaxDim,$loopyIPFSolve,$loopyIPFError,$loopyIPFEntropy, " +
        s"$difference_vanillaIPF_moment,$maxDifference_vanillaIPF_moment, $difference_effectiveIPF_vanillaIPF,$maxDifference_effectiveIPF_vanillaIPF, " +
        s"$difference_loopyIPF_vanillaIPF,$maxDifference_loopyIPF_vanillaIPF"
      fileout.println(resultrow)
    }
  }

  def entropy(p: Array[Double]): Double = {
    val grandTotal = p.sum
    p.map(n => if (n != 0) - (n/grandTotal) * math.log(n/grandTotal) else 0).sum
  }
}
