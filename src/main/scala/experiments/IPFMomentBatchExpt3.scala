package experiments

import core.DataCube
import core.solver.SolverTools
import core.solver.SolverTools.error
import core.solver.iterativeProportionalFittingSolver.EffectiveIPFSolver
import core.solver.moment.{CoMoment5Solver, CoMoment5SolverDouble, Moment1Transformer}
import planning.NewProjectionMetaData
import util.Profiler

class IPFMomentBatchExpt3(ename2: String = "")(implicit timestampedFolder: String) extends Experiment(s"ipf-moment-batch-partial", ename2, "ipf-expts") {
  var queryCounter = 0

  val header = "CubeName,Query,QSize,Fraction,   " +
  "MomentPrepare,MomentFetch,MomentSolve,MomentFetchSolve,MomentTotal,MomentError,   " +
  "IPFPrepare,IPFFetch,IPFCheckMissing,IPFSolve,IPFFetchSolve,IPFTotal,IPFError"
  fileout.println(header)

  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)]): Unit = {
    val q = qu.sorted

    List(0.1, 0.3, 1.0).foreach { f =>
      Profiler.resetAll()
      val moment = runMoment(dc, q, f)
      val ipf = runIPF(dc, q, f)

      if (output) {
        val momentPrepare = Profiler.getDurationMicro(s"Moment Prepare $f")
        val momentFetch = Profiler.getDurationMicro(s"Moment Fetch $f")
        val momentSolve = Profiler.getDurationMicro(s"Moment Solve $f")
        val momentFetchSolve = momentFetch + momentSolve
        val momentError = error(trueResult, moment.solution)
        val momentTotal = momentPrepare + momentFetch + momentSolve

        val ipfPrepare = Profiler.getDurationMicro(s"IPF Prepare $f")
        val ipfFetch = Profiler.getDurationMicro(s"IPF Fetch $f")
        val ipfCheckMissing = Profiler.getDurationMicro(s"IPF CheckMissing $f")
        val ipfSolve = Profiler.getDurationMicro(s"IPF Solve $f")
        val ipfFetchSolve = ipfFetch + ipfSolve
        val ipfTotal = ipfPrepare + ipfFetch + ipfCheckMissing + ipfSolve
        val ipfError = error(trueResult, ipf.solution)

        val resultRow = s"$dcname,${qu.mkString(":")},${q.length},$f,  " +
          s"$momentPrepare,$momentFetch,$momentSolve,$momentFetchSolve,$momentTotal,$momentError,   " +
          s"$ipfPrepare,$ipfFetch,$ipfCheckMissing,$ipfSolve,$ipfFetchSolve,$ipfTotal,$ipfError"

        fileout.println(resultRow)
      }
    }

  }


  def runMoment(dc: DataCube, q: IndexedSeq[Int], fractionOfCuboids: Double) = {
    val (l, pm) = Profiler(s"Moment Prepare $fractionOfCuboids") {
      val pm = SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
      val nbMinus1 = dc.index.n_bits - 1
      val cubs0 = dc.index.qproject(q, nbMinus1)

      val partialCubs = cubs0.sortBy(_.cuboidCost).take(math.ceil(cubs0.length * fractionOfCuboids).toInt).sorted(NewProjectionMetaData.ordering)
      val cubsER = dc.index.eliminateRedundant(partialCubs, nbMinus1).toList
      (cubsER, pm)
    }

    val maxDimFetch = l.last.cuboidCost
    val fetched = Profiler(s"Moment Fetch $fractionOfCuboids") { // Same as moment for now
      l.map { pmd => (pmd.queryIntersection, dc.fetch2[Double](List(pmd))) }
    }

    val solver = Profiler(s"Moment Solve $fractionOfCuboids") {
      val s = new CoMoment5SolverDouble(q.length, true, Moment1Transformer(), pm)
      fetched.foreach { case (bits, array) => s.add(bits, array) }
      s.fillMissing()
      s.solve(true)
      s
    }
    solver
  }

  def runIPF(dc: DataCube, q: IndexedSeq[Int], fractionOfCuboids: Double) = {
    val (l, pm) = Profiler(s"IPF Prepare $fractionOfCuboids") {
      val pm = SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
      val nbMinus1 = dc.index.n_bits - 1
      val cubs0 = dc.index.qproject(q, nbMinus1)

      val partialCubs = cubs0.sortBy(_.cuboidCost).take(math.ceil(cubs0.length * fractionOfCuboids).toInt).sorted(NewProjectionMetaData.ordering)
      val cubsER = dc.index.eliminateRedundant(partialCubs, nbMinus1).toList
      (cubsER, pm)
    }

    //Add any missing 1-D marginals if any, for fair comparison for IPF as other solvers have separate access to them anyway
    val missing1Dmarginals = Profiler(s"IPF CheckMissing $fractionOfCuboids") {
      val unionOfBits = l.map { _.queryIntersection }.reduce(_ | _)
      val total = pm.head._2
      pm.tail.flatMap { case (h, m) =>
        if ((unionOfBits & h) == 0)
          Some(h -> Array(total - m, m))
        else None
      }
    }

    val maxDimFetch = l.last.cuboidCost
    val fetched = Profiler(s"IPF Fetch $fractionOfCuboids") { // Same as moment for now
      missing1Dmarginals ++ l.map { pmd => (pmd.queryIntersection, dc.fetch2[Double](List(pmd))) }
    }

    val solver = Profiler(s"IPF Solve $fractionOfCuboids") {
      val s = new EffectiveIPFSolver(q.length)
      fetched.foreach { case (bits, array) => s.add(bits, array) }
      s.solve()
      s
    }
    solver
  }
}
