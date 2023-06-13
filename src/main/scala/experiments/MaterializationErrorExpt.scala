package experiments

import backend.CBackend
import combinatorics.Combinatorics
import core.solver.SolverTools
import core.solver.SolverTools.error
import core.solver.iterativeProportionalFittingSolver.EffectiveIPFSolver
import core.solver.moment.{CoMoment5SolverDouble, Moment1Transformer}
import core.{DataCube, PartialDataCube}
import frontend.generators.{StaticCubeGenerator, SSBSample}
import util.{Profiler, ProgressIndicator, Util}

class MaterializationErrorExpt(ename2: String = "")(implicit timestampedFolder: String) extends Experiment(s"ipf-moment-batch", ename2, "materialization") {
  var queryCounter = 0

  val header = "CubeName,Budget,DimLevel,Query,QSize,   " +
    "MomentPrepare,MomentFetch,MomentSolve,MomentFetchSolve,MomentTotal,MomentError,   " +
    "IPFPrepare,IPFFetch,IPFCheckMissing,IPFSolve,IPFFetchSolve,IPFTotal,IPFError"
  fileout.println(header)

  def budgetAndDimLevelFromName(name: String) = {
    val pieces = name.split("_")
    val kIdx = pieces.length - 1
    val bIdx = kIdx - 1
    val k = pieces(kIdx).toInt
    val b = pieces(bIdx).toDouble
    (b, k)
  }
  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)]): Unit = {
    val q = qu.sorted

      Profiler.resetAll()
      val moment = runMoment(dc, q)
      val ipf = runIPF(dc, q)

      if (output) {
        val momentPrepare = Profiler.getDurationMicro(s"Moment Prepare")
        val momentFetch = Profiler.getDurationMicro(s"Moment Fetch")
        val momentSolve = Profiler.getDurationMicro(s"Moment Solve")
        val momentFetchSolve = momentFetch + momentSolve
        val momentError = error(trueResult, moment.solution)
        val momentTotal = momentPrepare + momentFetch + momentSolve

        val ipfPrepare = Profiler.getDurationMicro(s"IPF Prepare")
        val ipfFetch = Profiler.getDurationMicro(s"IPF Fetch")
        val ipfCheckMissing = Profiler.getDurationMicro(s"IPF CheckMissing")
        val ipfSolve = Profiler.getDurationMicro(s"IPF Solve")
        val ipfFetchSolve = ipfFetch + ipfSolve
        val ipfTotal = ipfPrepare + ipfFetch + ipfCheckMissing + ipfSolve
        val ipfError = error(trueResult, ipf.solution)

        val (budget,dimlevel) = budgetAndDimLevelFromName(dcname)
        val resultRow = s"$dcname,$budget,$dimlevel,${qu.mkString(":")},${q.length},  " +
          s"$momentPrepare,$momentFetch,$momentSolve,$momentFetchSolve,$momentTotal,$momentError,   " +
          s"$ipfPrepare,$ipfFetch,$ipfCheckMissing,$ipfSolve,$ipfFetchSolve,$ipfTotal,$ipfError"

        fileout.println(resultRow)
      }
    }



  def runMoment(dc: DataCube, q: IndexedSeq[Int]) = {
    val (l, pm) = Profiler(s"Moment Prepare") {
      val pm = SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
      val cubs = dc.index.prepareBatch(q)
      (cubs, pm)
    }

    val fetched = Profiler(s"Moment Fetch") { // Same as moment for now
      l.map { pmd => (pmd.queryIntersection, dc.fetch2[Double](List(pmd))) }
    }

    val solver = Profiler(s"Moment Solve") {
      val s = new CoMoment5SolverDouble(q.length, true, Moment1Transformer(), pm)
      fetched.foreach { case (bits, array) => s.add(bits, array) }
      s.fillMissing()
      s.solve(true)
      s
    }
    solver
  }

  def runIPF(dc: DataCube, q: IndexedSeq[Int]) = {
    val (l, pm) = Profiler(s"IPF Prepare") {
      val pm = SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
      val cubs = dc.index.prepareBatch(q)
      (cubs, pm)
    }

    //Add any missing 1-D marginals if any, for fair comparison for IPF as other solvers have separate access to them anyway
    val missing1Dmarginals = Profiler(s"IPF CheckMissing") {
      val unionOfBits = l.map { _.queryIntersection }.reduce(_ | _)
      val total = pm.head._2
      pm.tail.flatMap { case (h, m) =>
        if ((unionOfBits & h) == 0)
          Some(h -> Array(total - m, m))
        else None
      }
    }

    val fetched = Profiler(s"IPF Fetch") { // Same as moment for now
      missing1Dmarginals ++ l.map { pmd => (pmd.queryIntersection, dc.fetch2[Double](List(pmd))) }
    }

    val solver = Profiler(s"IPF Solve") {
      val s = new EffectiveIPFSolver(q.length)
      fetched.foreach { case (bits, array) => s.add(bits, array) }
      s.solve()
      s
    }
    solver
  }
}
class MaterializationStatsExpt(ename2: String = "")(implicit timestampedFolder: String) extends Experiment(s"single-dim", ename2, "materialization") {
  val header = "CubeName,Budget,DimLevel,MS,  " +
    "TotalCuboids,NumMaterializedCuboids,NumDenseCuboids,NumSparseCuboids,ByteSizeCuboids,ActualBudgetRatio"
  fileout.println(header)
  def budgetDimLevelAndMSFromName(name: String) = {
    val pieces = name.split("_")
    val kIdx = pieces.length - 1
    val bIdx = kIdx - 1
    val k = pieces(kIdx).toInt
    val b = pieces(bIdx).toDouble
    val ms = pieces(bIdx-1)
    (b, k, ms)
  }
  def run2(dc: DataCube, dcname: String, cg: StaticCubeGenerator): Unit = {
    val (b, k, ms) = budgetDimLevelAndMSFromName(dcname)
    val n = cg.schemaInstance.n_bits
    val total = if(ms == "random")
      Combinatorics.comb(n, k).toDouble
    else {
      cg.schemaInstance.root.numPrefixUpto(k).last.toDouble
    }
    val projections = dc.cuboids.dropRight(1)
    val numMat = projections.size
    val be = projections.head.backend
    val numDense = projections.count(_.isInstanceOf[be.DenseCuboid])
    val numSparse = projections.count(_.isInstanceOf[be.SparseCuboid])
    val byteSize = projections.map(_.numBytes).sum
    val baseSize = dc.cuboids.last.numBytes
    val ratio = byteSize.toDouble/baseSize
    val outputrow = s"$dcname,$b,$k,$ms, " +
      s"$total,$numMat,$numDense,$numSparse,$byteSize,$ratio"
    fileout.println(outputrow)
  }
  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean, qname: String, sliceValues: Seq[(Int, Int)]): Unit = ???
}

object MaterializationErrorExpt {
  def main(args: Array[String]) = {
    implicit val be = CBackend.colstore
    implicit val timestampFolder = Experiment.now()
    implicit val numIters = 100
    val d0 = 20
    val cg = new SSBSample(d0)
    val base = cg.loadBase()
    val sch = cg.schemaInstance
    val strategy = "prefix"
    val queries = Vector(5, 10, 15).flatMap { qs =>
      //Util.collect_n_withAbort(numIters, () => sch.root.samplePrefix(qs).sorted, 10).toVector.map {
      Util.collect_n(numIters, () => scala.util.Random.shuffle((0 until sch.n_bits).toList).take(qs).toIndexedSeq.sorted).toVector.map{
        q => q -> base.naive_eval(q)
      }
    }.toMap
    //val expt = new MaterializationErrorExpt(cg.inputname + "_"+strategy)
    val expt2 = new MaterializationStatsExpt(cg.inputname + "_"+strategy)
    Vector(0.25, 1, 4, 16).foreach{ b =>
      println(s"Budget $b")
      (2 to d0).foreach { k =>
        val pi = new ProgressIndicator(queries.size, s"\n   Level $k")
        val cubename = cg.inputname + s"_${strategy}_${b}_$k"
        val dc = PartialDataCube.load(cubename, cg.baseName)
        dc.loadPrimaryMoments(cg.baseName)
        //queries.foreach{ case(q, trueRes) =>
        //  expt.run(dc, cubename, q, trueRes, sliceValues = Nil)
        //  pi.step
        //}
        expt2.run2(dc, cubename, cg)
        pi.step
        be.reset
      }
    }
  }
}

