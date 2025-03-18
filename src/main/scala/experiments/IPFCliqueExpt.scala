package experiments
import core.DataCube
import core.solver.SolverTools
import core.solver.SolverTools.{entropy, error}
import core.solver.iterativeProportionalFittingSolver.EffectiveIPFSolver
import core.solver.moment.{CoMoment5Solver, CoMoment5SolverDouble, Moment1Transformer}
import util.{BitUtils, Profiler}
class IPFCliqueExpt (ename2: String)(implicit shouldRecord: Boolean) extends Experiment("ipf-clique", ename2, "ipf-expts"){
  var queryCounter = 0
  val header = "CubeName,Query,Qsize,  " +
    "NumCuboidsFetched,FetchedCuboidSizes,  " +
    "TrueEntropy,EIPFEntropy,CM5Entropy,EIPFError,CM5Error,  " +
    "EIPFNumIterations,  " +
    "NumCliques,NumSeparators,CliqueSizes,CliqueClusterSizes,  " +
    "DOF,MuHistogram,  "
  fileout.println(header)
  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)]): Unit = {
    val q = qu.sorted
    val (fetched, pm, maxDimFetch) = momentPrepareFetch("Any", dc, q)

    val IPFResult = Profiler("Effective IPF Constructor + Add + Solve") {
      val solver = Profiler("Effective IPF Constructor") { new EffectiveIPFSolver(q.length) }
      Profiler("Effective IPF Add") { fetched.foreach { case (bits, array) => solver.add(bits, array) } }
      Profiler("Effective IPF Solve") { solver.solve() }
      solver
    }
    val momentResult = Profiler("Moment Solve") {
      val s = Profiler("Moment Constructor") {
        new CoMoment5SolverDouble(q.length, true, Moment1Transformer(), pm)
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
    val total = trueResult.sum
    var logh = 0

    val pmArray = new Array[Double](q.length)
    pm.tail.foreach { case (i, m) =>
      assert((1 << logh) == i)
      pmArray(logh) = (m / total)
      logh += 1
    }
    val trueMus = Moment1Transformer[Double]().getCoMoments(trueResult, pmArray)
    import math._
    def gbyfnc(mu: Double) = signum(mu).toInt -> (log(abs(mu)) / log(10)).toInt
    val unknownMus = (trueMus.indices.toSet.diff(momentResult.knownSet.toSet)).map(i => trueMus(i)/total)
    val unknownMuHist = unknownMus.groupBy(gbyfnc).mapValues(_.size).toList.map{ case ((s, lg), cnt) => s"$s:$lg:$cnt"}
    val jg = IPFResult.junctionGraph
    val trueEntropy = entropy(trueResult)
    val ipfEntropy = entropy(IPFResult.getSolution)
    val momentEntropy = entropy(momentResult.solution)
    val ipfError = error(trueResult, IPFResult.solution)
    val momentError = error(trueResult, momentResult.solution)

    val resultrow = s"$dcname,${qu.mkString(":")},${q.size},   "  +
      s"${fetched.length}, ${fetched.map{case (bits,_) => BitUtils.sizeOfSet(bits)}.mkString(":")},  " +
      s"${trueEntropy},$ipfEntropy,$momentEntropy,$ipfError,$momentError,  " +
      s"${IPFResult.numIterations},  " +
      s"${jg.cliques.size},${jg.separators.size},${jg.cliques.map(_.numVariables).mkString(":")},${jg.cliques.map(_.clusters.size).mkString(":")},   " +
      s"${unknownMus.size},${unknownMuHist.mkString("|")},  "
    fileout.println(resultrow)
    queryCounter += 1
  }

  /**
   * Prepare and fetch taken from the moment solver.
   * @param solverName The name of the concrete solver, to be used in entries in the profiler.
   * @param dc DataCube object.
   * @param q Sequence of queried dimensions.
   * @return The fetched cuboids and the maximum dimensionality.
   */
  def momentPrepareFetch(solverName: String, dc: DataCube, q: IndexedSeq[Int]) = {
    val (l, pm) = Profiler(s"$solverName Prepare") {
      dc.index.prepareBatch(q) -> SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
    }
    val maxDimFetch = l.last.cuboidCost
    val fetched = Profiler(s"$solverName Fetch") { // Same as moment for now
      l.map { pm => (pm.queryIntersection, dc.fetch2[Double](List(pm))) }
    }
    //println(s"\t\t\tNumber of cubes fetched: ${fetched.length}, Cube sizes (counts): " + s"${
    //  fetched.map { case (bits, _) => BitUtils.sizeOfSet(bits) }
    //    .groupBy(identity).mapValues(_.size).toList.sorted
    //    .map { case (cubeSizes, count) => s"$cubeSizes ($count)" }.mkString(", ")
    //}")
    (fetched, pm, maxDimFetch)
  }

}
