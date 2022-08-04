package experiments

import core.DataCube
import core.solver.SolverTools
import core.solver.SolverTools.entropy
import core.solver.iterativeProportionalFittingSolver._
import util.{BitUtils, Profiler}

import java.io.{File, PrintStream}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable

/**
 * The experiment to drop out one cuboid at a time and record the resulting time, error, and entropy at each step.
 * @author Zhekai Jiang
 */
class DropoutIPFExpt(ename2: String = "")(implicit shouldRecord: Boolean) extends Experiment("dropout-ipf", ename2) {

  val entropyBasedDropoutIPFFileout: PrintStream = {
    val isFinal = true
    val (timestamp, folder) = {
      if (isFinal) ("final", ".")
      else if (shouldRecord) {
        val datetime = LocalDateTime.now
        (DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").format(datetime), DateTimeFormatter.ofPattern("yyyyMMdd").format(datetime))
      } else ("dummy", "dummy")
    }
    val file = new File(s"expdata/$folder/entropy-based-${ename2}_$timestamp.csv")
    if (!file.exists())
      file.getParentFile.mkdirs()
    new PrintStream(file)
  }

  entropyBasedDropoutIPFFileout.println(
    "TrueEntropy, DroppedCubeDim, DroppedCubeEntropy, DroppedCubeMaxPossibleEntropy, DroppedCubeEntropyRatio, RemCoverage, "
      +"NumCliques, NumEntries, TotalTime(us), SolveTime(us), Err, Entropy"
  )

  val dimensionBasedDropoutIPFFileout: PrintStream = {
    val isFinal = true
    val (timestamp, folder) = {
      if (isFinal) ("final", ".")
      else if (shouldRecord) {
        val datetime = LocalDateTime.now
        (DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").format(datetime), DateTimeFormatter.ofPattern("yyyyMMdd").format(datetime))
      } else ("dummy", "dummy")
    }
    val file = new File(s"expdata/$folder/dimension-based-${ename2}_$timestamp.csv")
    if (!file.exists())
      file.getParentFile.mkdirs()
    new PrintStream(file)
  }

  dimensionBasedDropoutIPFFileout.println(
    "TrueEntropy, DroppedCubeDim, DroppedCubeEntropy, DroppedCubeMaxPossibleEntropy, DroppedCubeEntropyRatio, RemCoverage, "
      + "NumCliques, NumEntries, TotalTime(us), SolveTime(us), Err, Entropy"
  )


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
    val fetched = Profiler("Vanilla IPF Fetch") { // Same as moment for now
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
   * @param dcname Name of the data cube.
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
   * Solving with the effective version of IPF (with junction tree).
   * @param clusters Set of remaining clusters.
   * @param q Sequence of queried dimensions.
   * @param trueRes True result of the query.
   * @param dcname Name of the data cube.
   * @return Query result, maximum dimension fetched, number of cuboids fetched, sizes of fetched cuboids.
   */
  def effectiveIPF_solve(clusters: mutable.Set[Cluster], q: IndexedSeq[Int], trueRes: Array[Double], dcname: String): EffectiveIPFSolver = {
    val solver = Profiler("Effective IPF Constructor + Add + Solve") {
      val solver = Profiler("Effective IPF Constructor") { new EffectiveIPFSolver(q.length) }
      Profiler("Effective IPF Add") { clusters.foreach { case Cluster(bits, array) => solver.add(bits, array) } }
      Profiler("Effective IPF Solve") { solver.solve() }
      solver
    }
    solver
  }


  /**
   * Run experiment with the specified dropout policy.
   * @param policy "Entropy" (prioritize high entropy (ratio) cuboids) or "Dimension" (prioritize low dimensional cuboids).
   * @param dc The DataCube object.
   * @param q Sorted sequence of queries dimensions.
   * @param trueResult The true result of the query.
   * @param dcname Name of the data cube.
   */
  def run_dropoutIPF(policy: String, dc: DataCube, q: IndexedSeq[Int], trueResult: Array[Double], dcname: String): Unit = {
    println(s"$policy-Based")
    val (fetched, _) = momentPrepareFetch("Effective IPF", dc, q)
    val clusters = mutable.Set[Cluster]() ++ fetched.map { case (bits, array) => Cluster(bits, array) }

    val clustersQueue: mutable.PriorityQueue[Cluster] =
      if (policy == "Dimension")
        mutable.PriorityQueue[Cluster]()(Ordering.by(cluster =>
          - cluster.numVariables // low dimensionality to high dimensionality in case of tie
        )) ++ clusters
      else
        mutable.PriorityQueue[Cluster]()(Ordering.by(cluster => (
          entropyRatio (cluster), // high to low entropy
          - cluster.numVariables // low dimensionality to high dimensionality in case of tie
        ))) ++ clusters
    var totalCoverage = clusters.toList.map(_.numVariables).sum

    Profiler.resetAll()
    println("\tFull cuboids")
    val solver = effectiveIPF_solve(clusters, q, trueResult, dcname)
    val solveTime = Profiler.durations("Effective IPF Solve")._2 / 1000
    val totalTime = Profiler.durations("Effective IPF Total")._2 / 1000
    val error = SolverTools.error(trueResult, solver.getSolution)
    val solutionEntropy = entropy(solver.getSolution)
    println(s"\t\tSolve time: $solveTime, total time: $totalTime, solution error $error, entropy $solutionEntropy")
    (if (policy == "dimensionality") dimensionBasedDropoutIPFFileout else entropyBasedDropoutIPFFileout).println(
      s"${entropy(trueResult)}, , , , , $totalCoverage, "
        + s"${solver.junctionGraph.cliques.size}, ${solver.junctionGraph.cliques.toList.map(1 << _.numVariables).sum}, $totalTime, $solveTime, $error, $solutionEntropy"
    )

    var cuboidIndex = 0
    while (clustersQueue.nonEmpty) {
      cuboidIndex += 1
      Profiler.resetAll()
      val cluster = clustersQueue.dequeue()
      totalCoverage -= cluster.numVariables
      clusters -= cluster
      println(s"\tDropped cuboid $cuboidIndex/${fetched.size}: size ${cluster.numVariables}, entropy ${entropy(cluster.distribution)}, entropy ratio ${entropyRatio(cluster)}")
      val solver = effectiveIPF_solve(clusters, q, trueResult, dcname)
      val solveTime = Profiler.durations("Effective IPF Solve")._2 / 1000
      val totalTime = Profiler.durations("Effective IPF Total")._2 / 1000
      val error = SolverTools.error(trueResult, solver.getSolution)
      val solutionEntropy = entropy(solver.getSolution)
      println(s"\t\tSolve time: $solveTime, total time: $totalTime, solution error $error, entropy $solutionEntropy")
      (if (policy == "dimensionality") dimensionBasedDropoutIPFFileout else entropyBasedDropoutIPFFileout).println(
        s"${entropy(trueResult)}, ${cluster.numVariables}, ${entropy(cluster.distribution)}, ${-math.log(1.0 / (1 << cluster.numVariables))}, ${entropyRatio(cluster)}, $totalCoverage, "
          + s"${solver.junctionGraph.cliques.size}, ${solver.junctionGraph.cliques.toList.map(1 << _.numVariables).sum}, $totalTime, $solveTime, $error, $solutionEntropy"
      )
    }
  }


  def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: IndexedSeq[Int]): Unit = {
    run_dropoutIPF("Entropy", dc, qu.sorted, trueResult, dcname)
  }

  /**
   * Helper function to compute the ratio of the entropy of a cluster relative to the maximum possible entropy at its dimensionality.
   * @param cluster The cluster.
   * @return The ratio of the entropy of a cluster relative to the maximum possible entropy at its dimensionality.
   */
  def entropyRatio(cluster: Cluster): Double = entropy(cluster.distribution) / (-math.log(1.0 / (1 << cluster.numVariables)))
}
