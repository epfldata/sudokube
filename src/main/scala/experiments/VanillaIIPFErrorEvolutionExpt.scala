package experiments
import core.DataCube
import core.solver.iterativeProportionalFittingSolver.VanillaIPFSolver
import util.Profiler

/**
 * Record the change of solution error as vanilla IPF runs (check once for each cuboid update).
 * This will show that IPF may have already converged even before finishing one iteration.
 * @author Zhekai Jiang
 */
class VanillaIIPFErrorEvolutionExpt(ename2: String = "")(implicit shouldRecord: Boolean) extends Experiment("vanilla-ipf-error-evolution", ename2, "ipf-time-error") {
  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)]): Unit = {
    fileout.println("CubeName, Query, QSize, Iteration, IPFTotalTime(us), IPFSolveTime(us), IPFErr")
      // Time may actually not be useful here, because it includes the time to calculate error.
    val q = qu.sorted
    Profiler("Vanilla IPF Total") {
      val solver = new VanillaIPFSolver(q.size, true, true, trueResult, fileout, dcname, q.mkString(":"))
      val l = dc.index.prepareBatch(q)
      val fetched = l.map { pm => (pm.queryIntersection, dc.fetch2[Double](List(pm))) }
      fetched.foreach { case (bits, array) => solver.add(bits, array) }
      Profiler("Vanilla IPF Solve") {
        solver.solve()
      }
    }
  }
}
