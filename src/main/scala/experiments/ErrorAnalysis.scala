package experiments

import core.DataCube
import core.solver.SolverTools
import core.solver.iterativeProportionalFittingSolver.EffectiveIPFSolver
import core.solver.moment.{CoMoment5SolverDouble, Moment1Transformer}
import util.BitUtils

class ErrorAnalysis(ename2: String = "")(implicit timestampFolder: String) extends Experiment("moment-error", ename2, "error-expt") {
  fileout.println(
    "CubeName,Query,QSize,Idx,Order,OrderedIdx,  " +
      "TrueSol,PredictedSol,LocalError,  " +
      "TrueMu,ExtrapolatedMu,PredictedMu,MuIsKnown,  " +
      "TrueMuNorm,ExtrapolatedMuNorm,PredictedMuNorm,    " +
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
    val extrapolatedMu = solver.moments.clone()
    solver.solve()


    //convert ordinary moment to central moment
    var logh=0
    var h = 1
    val N = extrapolatedMu.size
    while(h < N) {
      val p = solver.pmArray(logh)
      var i = 0
      while(i < N) {
        var j = i
        while(j < i + h) {
          extrapolatedMu(j + h) -= extrapolatedMu(j) * p
          j += 1
        }
        i += (h << 1)
      }
      logh += 1
      h <<= 1
    }

    val ipf = new EffectiveIPFSolver(q.length)
    fetched.foreach { case (bits, array) => ipf.add(bits, array) }
    ipf.solve()
    ipf.getSolution

    val grandTotal = trueResult.sum
    val nf = 1.0/ grandTotal
    val localErrors = trueResult.indices.map { i => math.abs(trueResult(i) - solver.solution(i)) }
    val ipfLocalErrors = trueResult.indices.map { i => math.abs(trueResult(i) - ipf.solution(i)) }

    val trueMus = transformer.getCoMoments(trueResult, solver.pmArray)
    val baseNorms = solver.pmArray.map { p => 1.0/math.sqrt(p * (1.0 - p)) }
    val normProd = trueMus.indices.map { i => BitUtils.IntToSet(i).map(baseNorms(_)).product }
    val trueNorm = trueMus.indices.map { i => trueMus(i) * normProd(i) }

    val predMus = transformer.getCoMoments(solver.solution, solver.pmArray)
    val predMuNorm = predMus.indices.map { i => predMus(i) * normProd(i) }
    val extrapolatedMuNorm = extrapolatedMu.indices.map{ i => extrapolatedMu(i) * normProd(i)}
    val ipfMus = transformer.getCoMoments(ipf.solution, solver.pmArray)

    val muIsKnown = trueResult.indices.map { i => solver.knownSet.contains(i) }
    println("Moment Error = " + SolverTools.error(trueResult, solver.solution))
    println("IPF Error = " + SolverTools.error(trueResult, ipf.solution))

    import BitUtils.sizeOfSet
    val idx2 = trueResult.indices.map(i => i->sizeOfSet(i)).sortBy(_._2).
      zipWithIndex.map{ case ((iorig, order), inew) => (iorig, inew)}.sortBy(_._1).map(_._2)

    if (output) {
      trueResult.indices.foreach { idx =>
        fileout.println(
          s"$dcname,${qu.mkString(":")},${q.length},$idx,${sizeOfSet(idx)},${idx2(idx)},   " +
            s"${trueResult(idx) * nf},${solver.solution(idx) * nf},${localErrors(idx) * nf},  " +
            s"${trueMus(idx) * nf},${extrapolatedMu(idx) * nf}, ${predMus(idx) * nf},${muIsKnown(idx)},  " +
            s"${trueNorm(idx) * nf},${extrapolatedMuNorm(idx) * nf}, ${predMuNorm(idx) * nf},  " +
            s"${ipf.solution(idx) * nf}, ${ipfLocalErrors(idx) * nf}, ${ipfMus(idx) * nf}"

        )
      }
    }
  }

}
