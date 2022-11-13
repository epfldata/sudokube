package experiments

import core.DataCube
import core.solver.SolverTools
import core.solver.iterativeProportionalFittingSolver.EffectiveIPFSolver
import core.solver.moment.{CoMoment5SolverDouble, Moment1Transformer}

class ErrorAnalysis(ename2: String = "")(implicit timestampFolder: String) extends Experiment("moment-error", ename2, "error-expt") {
  fileout.println(
    "CubeName,Query,QSize,Idx,  " +
      "TrueSol,PredictedSol,LocalError,  " +
      "TrueMu,ExtrapolatedMu,MuIsKnown,  " +
       "IPFSol, IPFLocalError, IPFMu, "
  )
  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Vector()): Unit = {
    val q = qu.sorted
    val pm = SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
    val l = dc.index.prepareBatch(q)

    val fetched = l.map { pmd => (pmd.queryIntersection, dc.fetch2[Double](List(pmd))) }
    val transformer = Moment1Transformer[Double]()
    val solver = new CoMoment5SolverDouble(q.length, true, transformer, pm)
    fetched.foreach { case (bits, array) => solver.add(bits, array) }
    solver.fillMissing()
    solver.solve()

    val ipf = new EffectiveIPFSolver(q.length)
    fetched.foreach{ case (bits, array) => ipf.add(bits, array)}
    ipf.solve()
    ipf.getSolution

    val grandTotal = trueResult.sum
    val nf = 1.0 / grandTotal
    val localErrors = trueResult.indices.map { i => math.abs(trueResult(i) - solver.solution(i)) }
    val ipfLocalErrors = trueResult.indices.map{ i => math.abs(trueResult(i) - ipf.solution(i))}

    val trueMus = transformer.getCoMoments(trueResult, solver.pmArray)
    val predMus = transformer.getCoMoments(solver.solution, solver.pmArray)
    val ipfMus = transformer.getCoMoments(ipf.solution, solver.pmArray)

    val muIsKnown = trueResult.indices.map { i => solver.knownSet.contains(i) }
    println("Moment Error = " + SolverTools.error(trueResult, solver.solution))
    println("IPF Error = " + SolverTools.error(trueResult, ipf.solution))

    if (output) {
      trueResult.indices.foreach { idx =>
        fileout.println(
          s"$dcname,${qu.mkString(":")},${q.length},$idx,  " +
            s"${trueResult(idx) * nf},${solver.solution(idx) * nf},${localErrors(idx) * nf},  " +
            s"${trueMus(idx) * nf},${predMus(idx) * nf},${muIsKnown(idx)},  " +
            s"${ipf.solution(idx) * nf}, ${ipfLocalErrors(idx) * nf}, ${ipfMus(idx) * nf}"

        )
      }
    }
  }

}
