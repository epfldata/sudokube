package experiments

import core.DataCube
import core.solver.SolverTools
import core.solver.SolverTools.{entropy, error}
import core.solver.iterativeProportionalFittingSolver._
import core.solver.moment.{CoMoment5Solver, Moment1Transformer}
import util.{BitUtils, Profiler}
import BitUtils.sizeOfSet

import java.io.{File, PrintStream}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Compare the time and error for IPF solvers and moment solver.
 * Currently the loopy IPFs are not run (commented out) because the error tends to be very high and they may not converge.
 * @author Zhekai Jiang
 */
class IPFMomentBatchExpt(ename2: String = "")(implicit shouldRecord: Boolean) extends Experiment(s"ipf-moment-batch", ename2, "ipf-time-error") {
  fileout.println(
    "CubeName, MomentSolverName, Query, QSize, NCubesFetched, CubeSizes, DOF, TrueEntropy, " +

      // Moment
      "MTotalTime(us), MPrepareTime(us), MFetchTime(us), MSolveMaxDimFetched, MSolveTime(us), MErr, MEntropy, " +
      // Vanilla IPF
      "VIPFTotalTime(us), VIPFPrepareTime(us), VIPFFetchTime(us), VIPFMaxDimFetched, VIPFSolveTime(us), VIPFErr, VIPFEntropy, " +
      // Effective IPF
      "EIPFTotalTime(us), EIPFPrepareTime(us), EIPFFetchTime(us), EIPFMaxDimFetched, EIPFSolveTime(us), EIPFErr, EIPFEntropy, " +
      // Dimension-Based Dropout Effective IPF
      "DimDropoutIPFTotalTime(us), DimDropoutIPFPrepareTime(us), DimDropoutIPFFetchTime(us), DimDropoutIPFMaxDimFetched, DimDropoutIPFSolveTime(us), DimDropoutIPFErr, DimDropoutIPFEntropy, " +
      // Normalized-Entropy-Based Dropout Effective IPF
      "NormEntDropoutIPFTotalTime(us), NormEntDropoutIPFPrepareTime(us), NormEntDropoutIPFFetchTime(us), NormEntDropoutIPFMaxDimFetched, NormEntDropoutIPFSolveTime(us), NormEntDropoutIPFErr, NormEntDropoutIPFEntropy, " +
      // Smallest-Cliques Loopy IPF
//      "SCLIPFTotalTime(us), SCLIPFPrepareTime(us), SCLIPFFetchTime(us), SCLIPFMaxDimFetched, SCLIPFSolveTime(us), SCLIPFErr, SCLIPFEntropy, " +
      // Random Bucket Elimination Loopy IPF
//      "RBELIPFTotalTime(us), RBELIPFPrepareTime(us), RBELIPFFetchTime(us), RBELIPFMaxDimFetched, RBELIPFSolveTime(us), RBELIPFErr, RBELIPFEntropy, " +

      "Difference_vanillaIPF_vs_moment,MaxDifference_vanillaIPF_vs_moment, " +
      "Difference_effectiveIPF_vs_vanillaIPF,MaxDifference_effectiveIPF_vs_vanillaIPF, " // +
//      "Difference_loopyIPF_vs_vanillaIPF,MaxDifference_loopyIPF_vs_vanillaIPF"
  )

  /**
   * This is to roughly visualize the relationship between runtime and error rate for the vanilla IPF.
   */
  val ipfTimeErrorFileout: PrintStream = {
    val isFinal = true
    val (timestamp, folder) = {
      if (isFinal) ("final", ".")
      else if (shouldRecord) {
        val datetime = LocalDateTime.now
        (DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").format(datetime), DateTimeFormatter.ofPattern("yyyyMMdd").format(datetime))
      } else ("dummy", "dummy")
    }
    val file = new File(s"expdata/$folder/${ename2}_ipf-moment-time-error_$timestamp.csv")
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
  def moment_solve(dc: DataCube, q: IndexedSeq[Int]): (CoMoment5Solver[Double], Int) = {
    val (l, pm) = Profiler("Moment Prepare") {
      dc.index.prepareBatch(q) -> SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
    }
    val maxDimFetch = l.last.cuboidCost
    val fetched = Profiler("Moment Fetch") {
      l.map { pm =>
        (pm.queryIntersection, dc.fetch2[Double](List(pm)))
      }
    }

    val result = Profiler("Moment Solve") {
      val s = Profiler("Moment Constructor") {
        new CoMoment5Solver(q.length, true, Moment1Transformer[Double](), pm)
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
   * Prepare and fetch taken from the moment solver.
   * @param solverName The name of the concrete solver, to be used in entries in the profiler.
   * @param dc DataCube object.
   * @param q Sequence of queried dimensions.
   * @return The fetched cuboids and the maximum dimensionality.
   */
  def momentPrepareFetch(solverName: String, dc: DataCube, q: IndexedSeq[Int]): (List[(Int, Array[Double])], Int) = {
    val l = Profiler(s"$solverName Prepare") { // Not doing prepare for primary moments
      dc.index.prepareBatch(q)
    }
    val maxDimFetch = l.last.cuboidCost
    val fetched = Profiler(s"$solverName Fetch") { // Same as moment for now
      l.map { pm => (pm.queryIntersection, dc.fetch2[Double](List(pm))) }
    }
    println(s"\t\t\tNumber of cubes fetched: ${fetched.length}, Cube sizes (counts): " + s"${
      fetched.map{ case (bits, _) => BitUtils.sizeOfSet(bits) }
        .groupBy(identity).mapValues(_.size).toList.sorted
        .map { case (cubeSizes, count) => s"$cubeSizes ($count)" }.mkString(", ")
    }")
    (fetched, maxDimFetch)
  }


  /**
   * A common framework of the setup and execution of IPF solvers.
   * @param solverName The name of the concrete solver method, e.g. "Effective IPF".
   * @param solveMethod The solve method to be called, e.g. effectiveIPF_solve.
   * @param dc DataCube object.
   * @param q Sequence of queried dimensions.
   * @param trueResult True result of the query.
   * @param dcname Name of the data cube (optional, only used if we want to see it while experimenting).
   * @return Solver object, maximum dimension fetched, number of cuboids fetched, sizes of fetched cuboids, and error of the solution.
   */
  def runIPFSolver(solverName: String,
                   solveMethod: (DataCube, IndexedSeq[Int], Array[Double], String) => (IPFSolver, Int, Int, Seq[Int]),
                   dc: DataCube, q: IndexedSeq[Int], trueResult: Array[Double], dcname: String): (IPFSolver, Int, Int, Seq[Int], Double) = {
    println(s"\t\t$solverName starts")

    val (solver, maxDim, numCubesFetched, cubeSizes) = Profiler(s"$solverName Total") {
      solveMethod(dc, q, trueResult, dcname)
    }
    val error = Profiler(s"$solverName Error Checking") {
      SolverTools.error(trueResult, solver.getSolution)
    }

    println(s"\t\t\t$solverName solve time: " + Profiler.durations(s"$solverName Solve")._2 / 1000 +
      ", total time: " + Profiler.durations(s"$solverName Total")._2 / 1000 +
      ", error: " + error)

    (solver, maxDim, numCubesFetched, cubeSizes, error)
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
    val (fetched, maxDimFetch) = momentPrepareFetch("Vanilla IPF", dc, q)

    val result = Profiler("Vanilla IPF Constructor + Add + Solve") {
      val solver = Profiler("Vanilla IPF Constructor") {
        new VanillaIPFSolver(q.length, false, false, trueRes, ipfTimeErrorFileout, dcname, q.mkString(":"))
      }
      Profiler("Vanilla IPF Add") { fetched.foreach { case (bits, array) => solver.add(bits, array) } }
      Profiler("Vanilla IPF Solve") { solver.solve() }
      solver
    }
    (result, maxDimFetch, fetched.length, fetched.map { case (bits, _) => sizeOfSet(bits) })
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
    val (fetched, maxDimFetch) = momentPrepareFetch("Effective IPF", dc, q)

    val result = Profiler("Effective IPF Constructor + Add + Solve") {
      val solver = Profiler("Effective IPF Constructor") { new EffectiveIPFSolver(q.length) }
      Profiler("Effective IPF Add") { fetched.foreach { case (bits, array) => solver.add(bits, array) } }
      Profiler("Effective IPF Solve") { solver.solve() }
      solver
    }
    (result, maxDimFetch, fetched.length, fetched.map { case (bits, _) => sizeOfSet(bits) })
  }

  /**
   * Solving with the smallest-cliques loopy IPF (each cluster is in a distinct clique).
   * @param dc DataCube object.
   * @param q Sequence of queried dimensions.
   * @param trueRes True result of the query (optional, only used if we want to observe errors while experimenting).
   * @param dcname Name of the data cube (optional, only used if we want to see it while experimenting).
   * @return Query result, maximum dimension fetched, number of cuboids fetched, sizes of fetched cuboids.
   */
  def smallestCliquesLoopyIPF_solve(dc: DataCube, q: IndexedSeq[Int], trueRes: Array[Double], dcname: String): (SmallestCliquesLoopyIPFSolver, Int, Int, Seq[Int]) = {
    val (fetched, maxDimFetch) = momentPrepareFetch("Smallest-Cliques Loopy IPF", dc, q)

    val result = Profiler("Smallest-Cliques Loopy IPF Constructor + Add + Solve") {
      val solver = Profiler("Smallest-Cliques Loopy IPF Constructor") { new SmallestCliquesLoopyIPFSolver(q.length) }
      Profiler("Smallest-Cliques Loopy IPF Add") { fetched.foreach { case (bits, array) => solver.add(bits, array) } }
      Profiler("Smallest-Cliques Loopy IPF Solve") { solver.solve() }
      solver
    }
    (result, maxDimFetch, fetched.length, fetched.map { case (bits, _) => sizeOfSet(bits) })
  }

  /**
   * Solving with a junction graph where each cluster is placed randomly in one of the cliques that contains at least one intersecting variable.
   * @param dc DataCube object.
   * @param q Sequence of queried dimensions.
   * @param trueRes True result of the query (optional, only used if we want to observe errors while experimenting).
   * @param dcname Name of the data cube (optional, only used if we want to see it while experimenting).
   * @return Query result, maximum dimension fetched, number of cuboids fetched, sizes of fetched cuboids.
   */
  def randomBucketEliminationLoopyIPF_solve(dc: DataCube, q: IndexedSeq[Int], trueRes: Array[Double], dcname: String): (RandomBucketEliminationLoopyIPFSolver, Int, Int, Seq[Int]) = {
    val (fetched, maxDimFetch) = momentPrepareFetch("Random Bucket Elimination Loopy IPF", dc, q)

    val result = Profiler("Random Bucket Elimination Loopy IPF Constructor + Add + Solve") {
      val solver = Profiler("Random Bucket Elimination Loopy IPF Constructor") { new RandomBucketEliminationLoopyIPFSolver(q.length) }
      Profiler("Random Bucket Elimination Loopy IPF Add") { fetched.foreach { case (bits, array) => solver.add(bits, array) } }
      Profiler("Random Bucket Elimination Loopy IPF Solve") { solver.solve() }
      solver
    }
    (result, maxDimFetch, fetched.length, fetched.map { case (bits, _) => sizeOfSet(bits)})
  }

  /**
   * Solving with a junction tree where some low dimensional cuboids are thrown out.
   * @param dc DataCube object.
   * @param q Sequence of queried dimensions.
   * @param trueRes True result of the query (optional, only used if we want to observe errors while experimenting).
   * @param dcname Name of the data cube (optional, only used if we want to see it while experimenting).
   * @return Query result, maximum dimension fetched, number of cuboids fetched, sizes of fetched cuboids.
   */
  def dimensionBasedDropoutEffectiveIPF_solve(dc: DataCube, q: IndexedSeq[Int], trueRes: Array[Double], dcname: String): (DimensionBasedDropoutEffectiveIPFSolver, Int, Int, Seq[Int]) = {
    val (fetched, maxDimFetch) = momentPrepareFetch("Dimension-Based Dropout Effective IPF", dc, q)

    val result = Profiler("Dimension-Based Dropout Effective IPF Constructor + Add + Solve") {
      val solver = Profiler("Dimension-Based Dropout Effective IPF Constructor") {
        new DimensionBasedDropoutEffectiveIPFSolver(q.length)
      }
      Profiler("Dimension-Based Dropout Effective IPF Add") { fetched.foreach { case (bits, array) => solver.add(bits, array) } }
      Profiler("Dimension-Based Dropout Effective IPF Solve") { solver.solve() }
      solver
    }
    (result, maxDimFetch, fetched.length, fetched.map { case (bits, _) => sizeOfSet(bits) })
  }

  /**
   * Solving with a junction tree where some low dimensional cuboids are thrown out.
   * @param dc DataCube object.
   * @param q Sequence of queried dimensions.
   * @param trueRes True result of the query (optional, only used if we want to observe errors while experimenting).
   * @param dcname Name of the data cube (optional, only used if we want to see it while experimenting).
   * @return Query result, maximum dimension fetched, number of cuboids fetched, sizes of fetched cuboids.
   */
  def normalizedEntropyBasedDropoutEffectiveIPF_solve(dc: DataCube, q: IndexedSeq[Int], trueRes: Array[Double], dcname: String)
    : (NormalizedEntropyBasedDropoutEffectiveIPFSolver, Int, Int, Seq[Int]) = {
    val (fetched, maxDimFetch) = momentPrepareFetch("Normalized-Entropy-Based Dropout Effective IPF", dc, q)

    val result = Profiler("Normalized-Entropy-Based Dropout Effective IPF Constructor + Add + Solve") {
      val solver = Profiler("Normalized-Entropy-Based Dropout Effective IPF Constructor") {
        new NormalizedEntropyBasedDropoutEffectiveIPFSolver(q.length)
      }
      Profiler("Normalized-Entropy-Based Dropout Effective IPF Add") { fetched.foreach { case (bits, array) => solver.add(bits, array) } }
      Profiler("Normalized-Entropy-Based Dropout Effective IPF Solve") { solver.solve() }
      solver
    }
    (result, maxDimFetch, fetched.length, fetched.map { case (bits, _) => sizeOfSet(bits) })
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



    val (vanillaIPFSolver, vanillaIPFMaxDim, vanillaIPFNumCubesFetched, vanillaIPFCubeSizes, vanillaIPFError)
      = runIPFSolver("Vanilla IPF", vanillaIPF_solve, dc, q, trueResult, dcname)


    val (effectiveIPFSolver, effectiveIPFMaxDim, effectiveIPFNumCubesFetched, effectiveIPFCubeSizes, effectiveIPFError)
      = runIPFSolver("Effective IPF", effectiveIPF_solve, dc, q, trueResult, dcname)
    println(s"\t\t\tEffective IPF junction tree construction time: ${Profiler.durations("Effective IPF Junction Tree Construction")._2 / 1000}, "
      + s"iterations time: ${Profiler.durations("Effective IPF Iterations")._2 / 1000}, "
      + s"solution derivation time: ${Profiler.durations("Effective IPF Solution Derivation")._2 / 1000}")


    val (dimensionBasedDropoutEffectiveIPFSolver, dimensionBasedDropoutEffectiveIPFMaxDim, dimensionBasedDropoutEffectiveIPFNumCubesFetched, dimensionBasedDropoutEffectiveIPFCubeSizes, dimensionBasedDropoutEffectiveIPFError)
      = runIPFSolver("Dimension-Based Dropout Effective IPF", dimensionBasedDropoutEffectiveIPF_solve, dc, q, trueResult, dcname)
    println(s"\t\t\tDimension-Based Dropout Effective IPF junction tree construction time: ${Profiler.durations("Dimension-Based Dropout Effective IPF Junction Tree Construction")._2 / 1000}, "
      + s"iterations time: ${Profiler.durations("Dimension-Based Dropout Effective IPF Iterations")._2 / 1000}, "
      + s"solution derivation time: ${Profiler.durations("Dimension-Based Dropout Effective IPF Solution Derivation")._2 / 1000}")


    val (normalizedEntropyBasedDropoutEffectiveIPFSolver, normalizedEntropyBasedDropoutEffectiveIPFMaxDim, normalizedEntropyBasedDropoutEffectiveIPFNumCubesFetched, normalizedEntropyBasedDropoutEffectiveIPFCubeSizes, normalizedEntropyBasedDropoutEffectiveIPFError)
      = runIPFSolver("Normalized-Entropy-Based Dropout Effective IPF", normalizedEntropyBasedDropoutEffectiveIPF_solve, dc, q, trueResult, dcname)
    println(s"\t\t\tNormalized-Entropy-Based Dropout Effective IPF junction tree construction time: ${Profiler.durations("Normalized-Entropy-Based Dropout Effective IPF Junction Tree Construction")._2 / 1000}, "
      + s"iterations time: ${Profiler.durations("Normalized-Entropy-Based Dropout Effective IPF Iterations")._2 / 1000}, "
      + s"solution derivation time: ${Profiler.durations("Normalized-Entropy-Based Dropout Effective IPF Solution Derivation")._2 / 1000}")


//    val (smallestCliquesLoopyIPFSolver, smallestCliquesLoopyIPFMaxDim, smallestCliquesLoopyIPFNumCubesFetched, smallestCliquesLoopyIPFCubeSizes, smallestCliquesLoopyIPFError)
//      = runIPFSolver("Smallest-Cliques Loopy IPF", smallestCliquesLoopyIPF_solve, dc, q, trueResult, dcname)
//    println(s"\t\t\tSmallest-Cliques Loopy IPF junction graph construction time: ${Profiler.durations("Smallest-Cliques Loopy IPF Junction Graph Construction")._2 / 1000}, "
//      + s"iterations time: ${Profiler.durations("Smallest-Cliques Loopy IPF Iterations")._2 / 1000}, "
//      + s"solution derivation time: ${Profiler.durations("Smallest-Cliques Loopy IPF Solution Derivation")._2 / 1000}")


//    val (randomBucketEliminationLoopyIPFSolver, randomBucketEliminationLoopyIPFMaxDim, randomBucketEliminationLoopyIPFNumCubesFetched, randomBucketEliminationLoopyIPFCubeSizes, randomBucketEliminationLoopyIPFError)
//    = runIPFSolver("Random Bucket Elimination Loopy IPF", randomBucketEliminationLoopyIPF_solve, dc, q, trueResult, dcname)
//    println(s"\t\t\tRandom Bucket Elimination Loopy IPF junction graph construction time: ${Profiler.durations("Random Bucket Elimination Loopy IPF Junction Graph Construction")._2 / 1000}, "
//      + s"iterations time: ${Profiler.durations("Random Bucket Elimination Loopy IPF Iterations")._2 / 1000}, "
//      + s"solution derivation time: ${Profiler.durations("Random Bucket Elimination Loopy IPF Solution Derivation")._2 / 1000}")




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

//    val difference_loopyIPF_vanillaIPF = error(vanillaIPFSolver.getSolution, smallestCliquesLoopyIPFSolver.getSolution)
//    print(s"\t\t\tLoopy IPF vs vanilla IPF: $difference_loopyIPF_vanillaIPF ")

//    val maxDifference_loopyIPF_vanillaIPF = (0 until 1 << q.length).map(i => (vanillaIPFSolver.solution(i) - smallestCliquesLoopyIPFSolver.solution(i)).abs).max / grandTotal
//    println(s"Max difference out of total sum: $maxDifference_loopyIPF_vanillaIPF")


    val trueEntropy = entropy(trueResult)
    val momentEntropy = entropy(momentSolver.solution)
    val vanillaIPFEntropy = entropy(vanillaIPFSolver.getSolution)
    val effectiveIPFEntropy = entropy(effectiveIPFSolver.getSolution)
    val dimensionBasedDropoutEffectiveIPFEntropy = entropy(dimensionBasedDropoutEffectiveIPFSolver.getSolution)
    val normalizedEntropyBasedDropoutEffectiveIPFEntropy = entropy(normalizedEntropyBasedDropoutEffectiveIPFSolver.getSolution)
//    val smallestCliquesLoopyIPFEntropy = entropy(smallestCliquesLoopyIPFSolver.getSolution)
//    val randomBucketEliminationLoopyIPFEntropy = entropy(randomBucketEliminationLoopyIPFSolver.getSolution)
    println("\t\tEntropies")
    println(s"\t\t\tTrue Entropy = $trueEntropy")
    println(s"\t\t\tMoment Entropy = $momentEntropy")
    println(s"\t\t\tVanilla IPF Entropy = $vanillaIPFEntropy")
    println(s"\t\t\tEffective IPF Entropy = $effectiveIPFEntropy")
    println(s"\t\t\tDimension-Based Dropout Effective IPF Entropy = $dimensionBasedDropoutEffectiveIPFEntropy")
    println(s"\t\t\tNormalized-Entropy-Based Dropout Effective IPF Entropy = $normalizedEntropyBasedDropoutEffectiveIPFEntropy")
//    println(s"\t\t\tSmallest-Cliques Loopy IPF Entropy = $smallestCliquesLoopyIPFEntropy")
//    println(s"\t\t\tRandom Bucket Elimination Loopy IPF Entropy = $randomBucketEliminationLoopyIPFEntropy")

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

    val dimensionBasedDropoutEffectiveIPFPrepare = Profiler.durations("Dimension-Based Dropout Effective IPF Prepare")._2 / 1000
    val dimensionBasedDropoutEffectiveIPFFetch = Profiler.durations("Dimension-Based Dropout Effective IPF Fetch")._2 / 1000
    val dimensionBasedDropoutEffectiveIPFSolve = Profiler.durations("Dimension-Based Dropout Effective IPF Solve")._2 / 1000
    val dimensionBasedDropoutEffectiveIPFTotal = Profiler.durations("Dimension-Based Dropout Effective IPF Total")._2 / 1000

    val normalizedEntropyBasedDropoutEffectiveIPFPrepare = Profiler.durations("Normalized-Entropy-Based Dropout Effective IPF Prepare")._2 / 1000
    val normalizedEntropyBasedDropoutEffectiveIPFFetch = Profiler.durations("Normalized-Entropy-Based Dropout Effective IPF Fetch")._2 / 1000
    val normalizedEntropyBasedDropoutEffectiveIPFSolve = Profiler.durations("Normalized-Entropy-Based Dropout Effective IPF Solve")._2 / 1000
    val normalizedEntropyBasedDropoutEffectiveIPFTotal = Profiler.durations("Normalized-Entropy-Based Dropout Effective IPF Total")._2 / 1000

//    val smallestCliquesLoopyIPFPrepare = Profiler.durations("Smallest-Cliques Loopy IPF Prepare")._2 / 1000
//    val smallestCliquesLoopyIPFFetch = Profiler.durations("Smallest-Cliques Loopy IPF Fetch")._2 / 1000
//    val smallestCliquesLoopyIPFSolve = Profiler.durations("Smallest-Cliques Loopy IPF Solve")._2 / 1000
//    val smallestCliquesLoopyIPFTotal = Profiler.durations("Smallest-Cliques Loopy IPF Total")._2 / 1000

//    val randomBucketEliminationLoopyIPFPrepare = Profiler.durations("Random Bucket Elimination Loopy IPF Prepare")._2 / 1000
//    val randomBucketEliminationLoopyIPFFetch = Profiler.durations("Random Bucket Elimination Loopy IPF Fetch")._2 / 1000
//    val randomBucketEliminationLoopyIPFSolve = Profiler.durations("Random Bucket Elimination Loopy IPF Solve")._2 / 1000
//    val randomBucketEliminationLoopyIPFTotal = Profiler.durations("Random Bucket Elimination Loopy IPF Total")._2 / 1000

    if (output) {
      val resultrow = s"$dcname, ${momentSolver.name}, ${qu.mkString(":")},${q.size},$effectiveIPFNumCubesFetched,${effectiveIPFCubeSizes.mkString(":")},$dof,  " +
        s"$trueEntropy,  " +
        s"$mtot,$mprep,$mfetch,$momentMaxDim,$msolve,$momentError,$momentEntropy, " +
        s"$vanillaIPFTotal,$vanillaIPFPrepare,$vanillaIPFFetch,$vanillaIPFMaxDim,$vanillaIPFSolve,$vanillaIPFError,$vanillaIPFEntropy, " +
        s"$effectiveIPFTotal,$effectiveIPFPrepare,$effectiveIPFFetch,$effectiveIPFMaxDim,$effectiveIPFSolve,$effectiveIPFError,$effectiveIPFEntropy, " +
        s"$dimensionBasedDropoutEffectiveIPFTotal,$dimensionBasedDropoutEffectiveIPFPrepare,$dimensionBasedDropoutEffectiveIPFFetch,$dimensionBasedDropoutEffectiveIPFMaxDim,$dimensionBasedDropoutEffectiveIPFSolve,$dimensionBasedDropoutEffectiveIPFError,$dimensionBasedDropoutEffectiveIPFEntropy, " +
        s"$normalizedEntropyBasedDropoutEffectiveIPFTotal,$normalizedEntropyBasedDropoutEffectiveIPFPrepare,$normalizedEntropyBasedDropoutEffectiveIPFFetch,$normalizedEntropyBasedDropoutEffectiveIPFMaxDim,$normalizedEntropyBasedDropoutEffectiveIPFSolve,$normalizedEntropyBasedDropoutEffectiveIPFError,$normalizedEntropyBasedDropoutEffectiveIPFEntropy, " +
//        s"$smallestCliquesLoopyIPFTotal,$smallestCliquesLoopyIPFPrepare,$smallestCliquesLoopyIPFFetch,$smallestCliquesLoopyIPFMaxDim,$smallestCliquesLoopyIPFSolve,$smallestCliquesLoopyIPFError,$smallestCliquesLoopyIPFEntropy, " +
//        s"$randomBucketEliminationLoopyIPFTotal,$randomBucketEliminationLoopyIPFPrepare,$randomBucketEliminationLoopyIPFFetch,$randomBucketEliminationLoopyIPFMaxDim,$randomBucketEliminationLoopyIPFSolve,$randomBucketEliminationLoopyIPFError,$randomBucketEliminationLoopyIPFEntropy, " +
        s"$difference_vanillaIPF_moment,$maxDifference_vanillaIPF_moment, " +
        s"$difference_effectiveIPF_vanillaIPF,$maxDifference_effectiveIPF_vanillaIPF, " // +
//        s"$difference_loopyIPF_vanillaIPF,$maxDifference_loopyIPF_vanillaIPF"
      fileout.println(resultrow)
    }
  }
}
