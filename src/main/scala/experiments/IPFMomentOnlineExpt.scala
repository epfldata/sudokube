package experiments

import core.DataCube
import core.solver.SolverTools
import core.solver.SolverTools.{entropyBase2, error, normalizedEntropyBase2}
import core.solver.iterativeProportionalFittingSolver.{EffectiveIPFSolver, IPFUtils}
import core.solver.moment.{CoMoment5SolverDouble, Moment1TransformerDouble}
import util.{BitUtils, Profiler, ProgressIndicator}

class IPFMomentOnlineExpt(ename2: String)(implicit timestampedFolder: String) extends Experiment("ipf-moment-online", ename2, "ipf-expts") {
  fileout.println("QueryName,QuerySize,OrderByCol,FetchID,CuboidID,CuboidDim,Mu,Entropy,NormalizedEntropy,IPFError,CM5Error,IPFSolveTime,CM5SolveTime,IPFIterations")

  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: IndexedSeq[Int]): Unit = {
    val query = qu.sorted
    val primaryMoments = SolverTools.preparePrimaryMomentsForQuery[Double](query, dc.primaryMoments)
    val total = primaryMoments.head._2
    assert(primaryMoments.head._1 == 0)
    val pmMap = primaryMoments.map { case (i, m) => i -> m / total }.toMap
    val trueResult = dc.naive_eval(query)
    println(s"Online experiment for $dcname Query size = ${query.size}")
    println("Computing Marginals")
    val mus = Moment1TransformerDouble().getCoMoments(trueResult, pmMap)
    val marginals = trueResult.indices.map { i =>
      val size = BitUtils.sizeOfSet(i)
      val res = IPFUtils.getMarginalDistribution(query.length, trueResult, size, i)
      res
    }

    val entropies = marginals.map(x => entropyBase2(x))
    val normalizedEntropies = marginals.map(x => normalizedEntropyBase2(x))
    val alldata = trueResult.indices.map { i =>
      val size = BitUtils.sizeOfSet(i)
      (i, size, mus(i) / total, entropies(i), normalizedEntropies(i), marginals(i))
    }

    def onlineExpt(orderbyColName: String, data: Seq[(Int, Int, Double, Double, Double, Array[Double])]) = {
      val pi = new ProgressIndicator(data.size, s"Running experiment ordered by $orderbyColName")


      var cumulativeFetched = List[(Int, Array[Double])]()
      val total = primaryMoments.head._2

      val oneDcuboids = primaryMoments.tail.zipWithIndex.map { case ((h, v), logh) =>
        h -> Array(total - v, v)
      }

      cumulativeFetched ++= oneDcuboids //add 1D marginals (mainly for IPF as moment solver already has access to them)
      data.zipWithIndex.foreach { case ((i, si, mu, e, ne, marginal), idx) =>
        val filteredSubsets = cumulativeFetched.filter(x => ((x._1 & i) != x._1) || (x._1 == i)) //we remove proper subsets of i
        val existsSuperset = cumulativeFetched.exists(x => (x._1 & i) == i)
        cumulativeFetched = if (existsSuperset) filteredSubsets else (i -> marginal) :: filteredSubsets

        Profiler.resetAll()
        val eipf = Profiler("IPF Solve") {
          val eipfsolver = new EffectiveIPFSolver(query.length)
          cumulativeFetched.foreach { case (bits, values) =>
            eipfsolver.add(bits, values)
          }
          eipfsolver.solve()
          eipfsolver
        }

        val cm5 = Profiler("CM5 Solve") {
          val cm5solver = new CoMoment5SolverDouble(query.length, true, Moment1TransformerDouble(), primaryMoments)
          cumulativeFetched.foreach { case (bits, values) =>
            cm5solver.add(bits, values)
          }
          cm5solver.fillMissing()
          cm5solver.solve()
          cm5solver
        }

        val ipfError = error(trueResult, eipf.solution)
        val cm5Error = error(trueResult, cm5.solution)
        val ipfSolveTime = Profiler.getDurationMicro("IPF Solve")
        val cm5SolveTime = Profiler.getDurationMicro("CM5 Solve")
        val ipfiters = eipf.numIterations
        fileout.println(s"$qname,${query.length},$orderbyColName,$idx,$i,$si,$mu,$e,$ne,$ipfError,$cm5Error,$ipfSolveTime,$cm5SolveTime,$ipfiters")
        pi.step
      }
    }


    onlineExpt("cuboidsize", alldata.sortBy { case (i, si, mu, e, ne, marginal) => si })
    onlineExpt("muabs", alldata.sortBy { case (i, si, mu, e, ne, marginal) => -math.abs(mu) })
    onlineExpt("entropy", alldata.sortBy { case (i, si, mu, e, ne, marginal) => e })
  }


}
