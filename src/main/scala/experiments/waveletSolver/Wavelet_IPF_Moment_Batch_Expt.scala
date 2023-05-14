package experiments.waveletSolver

import core.DataCube
import core.solver.SolverTools
import core.solver.SolverTools.{entropy, error}
import core.solver.iterativeProportionalFittingSolver.VanillaIPFSolver
import core.solver.moment.{CoMoment5SolverDouble, Moment1Transformer}
import core.solver.simple.{AverageSolver, OneDProductSolver}
import core.solver.wavelet.{CoefficientAveragingWaveletSolver, CoefficientSelectionWaveletSolver}
import experiments.Experiment
import util.{BitUtils, Profiler}

class Wavelet_IPF_Moment_Batch_Expt(ename2: String = "")(implicit timestampedFolder: String)
  extends Experiment(s"wavelet-ipf-moment-simple-batch", ename2, "wavelet-expts") {

  var queryCounter = 0

  override def run(dc: DataCube, dcName: String, qu: IndexedSeq[Int], trueResult: Array[Double],
                   output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)])
  : Unit = {
    val q = qu.sorted

    Profiler.resetAll()

    val (fetched, pm, _) = prepareFetch(dc, q)

    val solvers = Map(
      "CoefficientSelectionWavelet" -> Profiler("CoefficientSelectionWavelet Solve") {
        val s = new CoefficientSelectionWaveletSolver(q.length)
        fetched.foreach { case (bits, array) => s.addCuboid(bits, array) }
        s.solve()
        s
      },
      "CoefficientAveragingWavelet" -> Profiler("CoefficientAveragingWavelet Solve") {
        val s = new CoefficientAveragingWaveletSolver(q.length)
        fetched.foreach { case (bits, array) => s.addCuboid(bits, array) }
        s.solve()
        s
      },
      "CM5NoFix" -> Profiler("CM5NoFix Solve") {
        val s = new CoMoment5SolverDouble(q.length, true, Moment1Transformer(), pm)
        fetched.foreach { case (bits, array) => s.add(bits, array) }
        s.fillMissing()
        s.solve(false)
        s
      },
      "CM5WithFix" -> Profiler("CM5WithFix Solve") {
        val s = new CoMoment5SolverDouble(q.length, true, Moment1Transformer(), pm)
        fetched.foreach { case (bits, array) => s.add(bits, array) }
        s.fillMissing()
        s.solve(true)
        s
      },
      "vIPF" -> Profiler("VIPF Solve") {
        val s = new VanillaIPFSolver(q.length)
        fetched.foreach { case (bits, array) => s.add(bits, array) }
        s.solve()
        s
      },
      "oneD" -> Profiler("OneD Solve") {
        val s = new OneDProductSolver(q.length, pm)
        s.solve()
        s
      },
      "avg" -> Profiler("Avg Solve") {
        val s = new AverageSolver(q.length, pm)
        fetched.foreach { case (bits, array) => s.add(bits, array) }
        s.solve()
        s
      }
    )

    val prepareTime = Profiler.getDurationMicro("Prepare")
    val fetchTime = Profiler.getDurationMicro("Fetch")

    val solveTimes = solvers.map { case (name, _) => name -> Profiler.getDurationMicro(name + " Solve") }

    val entropies = solvers.map { case (name, solver) =>
      val solution = solver.asInstanceOf[ {var solution: Array[Double]}].solution
      name -> entropy(solution)
    }

    if (output) {
      val errors = solvers.map { case (name, solver) =>
        val solution = solver.asInstanceOf[ {var solution: Array[Double]}].solution
        name -> error(trueResult, solution)
      }

      val total = trueResult.sum

      val CM5SolverWithFix = solvers("CM5WithFix").asInstanceOf[CoMoment5SolverDouble]
      val allmus = Moment1Transformer[Double]().getCoMoments(trueResult, CM5SolverWithFix.pmArray)
      val dof = CM5SolverWithFix.dof
      val unknownMus = allmus.indices.filter(i => !CM5SolverWithFix.knownSet.contains(i)).map(i => math.abs(allmus(i) / total))
      val countMax = 16
      val muCounts = Array.fill(countMax)(0)
      unknownMus.foreach { mu =>
        val log = -math.floor(math.log10(mu))
        val idx = if (log >= countMax) countMax - 1 else log.toInt
        muCounts(idx) += 1
      }

      val header: String = "CubeName,Query,QSize,  " +
        "Prepare(us),Fetch(us),NumFetched,FetchedSizes,  " +
        solvers.keys.map(s => s"${s}Solve(us)").mkString(",") + ",  " +
        solvers.keys.map(s => s"${s}Error").mkString(",") + ",  " +
        solvers.keys.map(s => s"${s}Entropy").mkString(",") + ",  " +
        "DOF,unknownMuDist"

      fileout.println(header)

      val resultRow = s"$dcName,${qu.mkString(":")},${q.length},  " +
        s"$prepareTime,$fetchTime,${fetched.length},${fetched.map(x => BitUtils.sizeOfSet(x._1)).mkString(":")},   " +
        solveTimes.values.mkString(",") + ",  " +
        errors.values.mkString(",") + ",  " +
        entropies.values.mkString(",") + ",  " +
        s"$dof,${muCounts.mkString(":")}"

      fileout.println(resultRow)
    }
  }

  /**
   * Prepare and fetch taken from the moment solver.
   *
   * @param dc DataCube object.
   * @param q  Sequence of queried dimensions.
   * @return The fetched cuboids and the maximum dimensionality.
   */
  def prepareFetch(dc: DataCube, q: IndexedSeq[Int]): (Seq[(Int, Array[Double])], Seq[(Int, Double)], Int) = {
    val (l, pm) = Profiler("Prepare") {
      dc.index.prepareBatch(q) -> SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
    }

    //Add any missing 1-D marginals if any, for fair comparison for IPF as other solvers have separate access to them anyway
    val unionOfBits = l.map {
      _.queryIntersection
    }.reduce(_ | _)
    val total = pm.head._2
    val missing1Dmarginals = pm.tail.flatMap { case (h, m) =>
      if ((unionOfBits & h) == 0)
        Some(h -> Array(total - m, m))
      else None
    }

    val maxDimFetch = l.last.cuboidCost
    val fetched = Profiler(s"Fetch") { // Same as moment for now
      missing1Dmarginals ++ l.map { pmd => (pmd.queryIntersection, dc.fetch2[Double](List(pmd))) }
    }
    //println(s"\t\t\tNumber of cubes fetched: ${fetched.length}, Cube sizes (counts): " + s"${
    //  fetched.map { case (bits, _) => BitUtils.sizeOfSet(bits) }
    //    .groupBy(identity).mapValues(_.size).toList.sorted
    //    .map { case (cubeSizes, count) => s"$cubeSizes ($count)" }.mkString(", ")
    //}")
    (fetched, pm, maxDimFetch)
  }
}
