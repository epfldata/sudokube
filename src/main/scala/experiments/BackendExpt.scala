package experiments

import backend.CBackend
import core.DataCube
import core.solver.SolverTools
import core.solver.SolverTools.error
import core.solver.moment.{CoMoment5SolverDouble, Moment1Transformer}
import util.Profiler

class BackendExpt(ename2: String = "")(implicit timestampedFolder: String) extends Experiment(s"triestore", ename2, "materialization-expts") {
  val header = "CubeName,Query,QSize,  " +
    "Prepare(us),Fetch(us),Solve(us),Error"
  fileout.println(header)
  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)]): Unit = {
    val q = qu.sorted
    Profiler.resetAll()
    val (fetched, pm, maxDimFetch) = prepareFetch(dc, q)

    val s = Profiler("Solve") {
      val s = new CoMoment5SolverDouble(q.length, true, Moment1Transformer(), pm, true)
      fetched.foreach { case (bits, array) => s.add(bits, array) }
      s.fillMissing()
      s.solve(true)
      s
    }

    val prepareTime = Profiler.getDurationMicro("Prepare")
    val fetchTime = Profiler.getDurationMicro("Fetch")
    val solve = Profiler.getDurationMicro("Solve")

    if (output) {
      val err = error(trueResult, s.solution)
      val resultRow = s"$dcname,${qu.mkString(":")},${q.length},   " +
        s"$prepareTime,$fetchTime,$solve,$err"
      fileout.println(resultRow)
    }

  }
  def runTrie(primaryMoments: (Long, Array[Long]), dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean, qname: String, sliceValues: Seq[(Int, Int)]): Unit = {
    val q = qu.sorted
    Profiler.resetAll()

    val pm = Profiler("Prepare") {
      SolverTools.preparePrimaryMomentsForQuery[Double](q, primaryMoments)
    }

    val moments = Profiler("Fetch") {
      val slice2 = Array.fill(q.size)(-1)
      sliceValues.foreach { case (i, v) => slice2(i) = v }
      CBackend.triestore.prepareFromTrie(q, slice2)
    }

    val s = Profiler("Solve") {
      val s = new CoMoment5SolverDouble(q.length, true, Moment1Transformer(), pm, true)
      s.moments = moments
      s.fillMissing()
      s.solve(true)
      s
    }

    val prepareTime = Profiler.getDurationMicro("Prepare")
    val fetchTime = Profiler.getDurationMicro("Fetch")
    val solve = Profiler.getDurationMicro("Solve")

    if (output) {
      val err = error(trueResult, s.solution)
      val resultRow = s"$dcname,${qu.mkString(":")},${q.length},   " +
        s"$prepareTime,$fetchTime,$solve,$err"
      fileout.println(resultRow)
    }
  }

  def prepareFetch(dc: DataCube, q: IndexedSeq[Int]) = {
    val (l, pm) = Profiler("Prepare") {
      dc.index.prepareBatch(q) -> SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
    }
    val maxDimFetch = l.last.cuboidCost
    val fetched = Profiler(s"Fetch") { // Same as moment for now
      l.map { pmd => (pmd.queryIntersection, dc.fetch2[Double](List(pmd))) }
    }
    (fetched, pm, maxDimFetch)
  }
}
