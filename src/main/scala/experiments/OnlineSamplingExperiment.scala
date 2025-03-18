package experiments

import backend.CBackend
import core.{DataCube, MaterializedQueryResult}
import core.solver.SolverTools
import core.solver.iterativeProportionalFittingSolver.{MSTVanillaIPFSolver, VanillaIPFSolver}
import core.solver.moment.CoMoment5SolverDouble
import core.solver.sampling.{IPFSamplingSolver, MomentSamplingSolver, NaiveSamplingSolver}
import frontend.experiments.Tools
import frontend.generators._
import util.{Profiler, ProgressIndicator}

import java.io.{File, PrintStream}

class OnlineSamplingExperiment(ename2: String = "")(implicit timestampedFolder: String) extends Experiment(s"ipf-moment", ename2, "online-sampling") {
  val header = "CubeName,Query,QSize," +
    "Solver,FractionOfSamples,ElapsedTotalTime,PrepareTime,FetchTime,SolveTime,L1Error,L1ErrorSlice,LinfError,KnownSetSize"
  fileout.println(header)
  val solout = new PrintStream(s"expdata/solution.csv")
  def runNaiveBatch(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double]) = {
    val algo = "NaiveBatch"
    println(s"Running $algo")
    val q = qu.sorted
    Profiler.resetAll()
    val cub = Profiler(s"Prepare") { dc.index.prepareNaive(q) }
    println(s"\t  Prepare Done")
    val fetched = Profiler(s"Fetch") { dc.fetch2[Double](cub) }
    println(s"\t  Fetch Done")
    val error = SolverTools.error(trueResult, fetched) //should be 0
    val prepareTime = Profiler.getDurationMicro(s"Prepare")
    val fetchTime = Profiler.getDurationMicro(s"Fetch")
    val totalTime = prepareTime + fetchTime
    val outputrow = s"$dcname,${qu.mkString(":")},${qu.size}," +
      s"$algo,1.0,$totalTime,$prepareTime,$fetchTime,0.0,$error"
    solout.println("Naive," + fetched.mkString(","))
    fileout.println(outputrow)
  }
  def runMomentBatch(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double]) = {
    val algo = "MomentBatch"
    val q = qu.sorted
    Profiler.resetAll()
    println(s"Running $algo")
    val (list, primMoments) = Profiler(s"Prepare") { dc.index.prepareBatch(q) -> SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments) }
    println(s"\t  Prepare Done")
    val fetched = Profiler(s"Fetch") { list.map { pm => pm.queryIntersection -> dc.fetch2[Double](List(pm)) } }
    println(s"\t  Fetch Done")
    val result = Profiler(s"Solve") {
      val s = new CoMoment5SolverDouble(q.size, true, null, primMoments)
      fetched.foreach { case (bits, array) => s.add(bits, array) }
      s.fillMissing()
      s.solve()
    }
    solout.println("Moment," + result.mkString(","))
    println(s"\t  Solve Done")
    val error = SolverTools.error(trueResult, result)
    val prepareTime = Profiler.getDurationMicro(s"Prepare")
    val fetchTime = Profiler.getDurationMicro(s"Fetch")
    val solveTime = Profiler.getDurationMicro(s"Solve")
    val totalTime = prepareTime + fetchTime + solveTime
    val outputrow = s"$dcname,${qu.mkString(":")},${qu.size}," +
      s"$algo,0.0,$totalTime,$prepareTime,$fetchTime,$solveTime,$error"
    fileout.println(outputrow)
  }
  def runIPFBatch(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double]) = {
    val algo = "VanillaIPFBatch"
    val q = qu.sorted
    Profiler.resetAll()
    println(s"Running $algo")
    val list = Profiler(s"Prepare") { dc.index.prepareBatch(q) }
    println(s"\t  Prepare Done")
    val fetched = Profiler(s"Fetch") { list.map { pm => pm.queryIntersection -> dc.fetch2[Double](List(pm)) } }
    println(s"\t  Fetch Done")
    val result = Profiler(s"Solve") {
      val s = new VanillaIPFSolver(q.size)
      fetched.foreach { case (bits, array) => s.add(bits, array) }
      s.solve()
    }
    println(s"\t  Solve Done")
    val error = SolverTools.error(trueResult, result)
    val prepareTime = Profiler.getDurationMicro(s"Prepare")
    val fetchTime = Profiler.getDurationMicro(s"Fetch")
    val solveTime = Profiler.getDurationMicro(s"Solve")
    val totalTime = prepareTime + fetchTime + solveTime
    val outputrow = s"$dcname,${qu.mkString(":")},${qu.size}," +
      s"$algo,0.0,$totalTime,$prepareTime,$fetchTime,$solveTime,$error"
    fileout.println(outputrow)
  }
  def runIPF2Batch(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double]) = {
    val algo = "MSTIPFBatch"
    val q = qu.sorted
    Profiler.resetAll()
    println(s"Running $algo")
    val list = Profiler(s"Prepare") { dc.index.prepareBatch(q) }
    println(s"\t  Prepare Done")
    val fetched = Profiler(s"Fetch") { list.map { pm => pm.queryIntersection -> dc.fetch2[Double](List(pm)) } }
    println(s"\t  Fetch Done")
    val result = Profiler(s"Solve") {
      val s = new MSTVanillaIPFSolver(q.size)
      fetched.foreach { case (bits, array) => s.add(bits, array) }
      s.solve()
    }
    println(s"\t  Solve Done")
    val error = SolverTools.error(trueResult, result)
    val prepareTime = Profiler.getDurationMicro(s"Prepare")
    val fetchTime = Profiler.getDurationMicro(s"Fetch")
    val solveTime = Profiler.getDurationMicro(s"Solve")
    val totalTime = prepareTime + fetchTime + solveTime
    val outputrow = s"$dcname,${qu.mkString(":")},${qu.size}," +
      s"$algo,0.0,$totalTime,$prepareTime,$fetchTime,$solveTime,$error"
    fileout.println(outputrow)
  }
  def runNaiveOnline(groupSize: Int)(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], sliceValues: Seq[(Int, Int)]) = {
    val algo = s"NaiveOnline-$groupSize"
    val q = qu.sorted
    Profiler.resetAll()
    println(s"Running $algo")
    val (solver, cuboid, mask) = Profiler(s"Prepare") {
      val pm = dc.index.prepareNaive(q).head
      val be = dc.cuboids.head.backend
      val cuboid = dc.cuboids(pm.cuboidID).asInstanceOf[be.SparseCuboid]
      assert(cuboid.n_bits == dc.index.n_bits)
      val s = new NaiveSamplingSolver(q.size, cuboid.size.toDouble)
      (s, cuboid, pm.cuboidIntersection)
    }
    println(s"\t  Prepare Done")
    val prepareTime = Profiler.getDurationMicro("Prepare")
    val common = s"$dcname,${qu.mkString(":")},${qu.size},$algo,"
    val initSolveTime = 0.0
    val initFetchTime = 0.0
    val initL1Error = 1.0
    val initL1ErrorSlice = 1.0
    val initLinfError = trueResult.max
    fileout.println(common + s"0.0,$prepareTime,$prepareTime,$initFetchTime, $initSolveTime,$initL1Error,$initL1ErrorSlice,$initLinfError")

    val numWords = ((cuboid.size + 63) >> 6).toInt
    val numGroups = numWords >> groupSize
    val pi = new ProgressIndicator(numGroups, "\t Sampling", false)
    Profiler("Sampling") {
    (0 to numGroups).foreach { i =>
      Profiler("Fetch") {
        (0 until 1 << groupSize).foreach { j =>
          val s = cuboid.projectFetch64((i << groupSize) + j, mask)
          solver.addSample(s)
        }
      }
      val result = Profiler("Solve") { solver.solve() }
      println(s"Sample $i/$numGroups " + result.mkString(" "))
      val fetchTime = Profiler.getDurationMicro("Fetch")
      val solveTime = Profiler.getDurationMicro("Solve")
      val totalTime = prepareTime + fetchTime + solveTime
      val errorfull = SolverTools.error(trueResult, result)
      def slice(res: Array[Double]) = util.Util.slice(res, sliceValues)
      val errorslice = SolverTools.error(slice(trueResult), slice(result))
      println("\t Slice values " + slice(trueResult).mkString("[", " ", "]" ) +  "  " + slice(result).mkString("[", " ", "]" ))
      val errorMax = SolverTools.errorMax(trueResult, result)
      val fraction = (i + 1).toDouble / (numGroups + 1)
      fileout.println(common + s"$fraction,$totalTime,$prepareTime,$fetchTime,$solveTime,$errorfull,$errorslice,$errorMax")
      pi.step
    }
  }
    Profiler.print()
  }
  def runMomentOnline(groupSize: Int, version: String)(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double]) = {
    val algo = s"MomentOnline-$version-$groupSize"
    val q = qu.sorted
    Profiler.resetAll()
    println(s"Running $algo")
    val (solver, cuboid, mask, pms) = Profiler(s"Prepare") {
      val pm = dc.index.prepareNaive(q).head
      val be = dc.cuboids.head.backend
      val cuboid = dc.cuboids(pm.cuboidID).asInstanceOf[be.SparseCuboid]
      assert(cuboid.n_bits == dc.index.n_bits)
      val primMoments = SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
      val s = new MomentSamplingSolver(q.size, primMoments, version, cuboid.size.toDouble)
      val pms = dc.index.prepareBatch(q)
      (s, cuboid, pm.cuboidIntersection, pms)
    }
    println(s"\t  Prepare Done")
    val initResult = if(version == "V1") {
      val fetched = Profiler("Fetch") {
        pms.map { pm => pm.queryIntersection -> dc.fetch2[Double](List(pm)) }
      }
      println(s"\t  BatchFetch Done")
      Profiler("Solve") {
        fetched.foreach { case (bits, array) => solver.addCuboid(bits, array) }
        solver.solve()
      }
    } else {
      Array.fill(trueResult.size)(0.0)
    }
    println(s"\t  BatchSolve Done")
    val initError = SolverTools.error(trueResult, initResult)
    val prepareTime = Profiler.getDurationMicro("Prepare")
    def fetchTime = Profiler.getDurationMicro("Fetch")
    def solveTime = Profiler.getDurationMicro("Solve")
    def totalTime = prepareTime + fetchTime + solveTime
    val common = s"$dcname,${qu.mkString(":")},${qu.size},$algo,"
    fileout.println(common + s"0.0,$totalTime,$prepareTime,$fetchTime,$solveTime,$initError")

    val numWords = ((cuboid.size + 63) >> 6).toInt
    val numGroups = numWords >> groupSize
    val pi = new ProgressIndicator(numGroups,"\t Sampling")
    (0 to numGroups).foreach { i =>
      Profiler("Fetch") {
        (0 until 1 << groupSize).foreach { j =>
          val s = cuboid.projectFetch64((i << groupSize) + j, mask)
          solver.addSample(s)
        }
      }
      val result = Profiler("Solve") { solver.solve() }
      val error = SolverTools.error(trueResult, result)
      val errorMax = SolverTools.errorMax(trueResult, result)
      val fraction = (i+1).toDouble/(numGroups + 1)
      fileout.println(common + s"$fraction,$totalTime,$prepareTime,$fetchTime,$solveTime,$error,$errorMax,${solver.knownSet.size}")
      pi.step
    }
  }
  def runIPFOnline(groupSize: Int, version: String)(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double]) = {
    val algo = s"MSTIPFOnline-$version-$groupSize"
    val q = qu.sorted
    Profiler.resetAll()
    println(s"Running $algo")
    val (solver, cuboid, mask, pms) = Profiler(s"Prepare") {
      val pm = dc.index.prepareNaive(q).head
      val be = dc.cuboids.head.backend
      val cuboid = dc.cuboids(pm.cuboidID).asInstanceOf[be.SparseCuboid]
      assert(cuboid.n_bits == dc.index.n_bits)
      val s = new IPFSamplingSolver(q.size, version)
      val pms = dc.index.prepareBatch(q)
      (s, cuboid, pm.cuboidIntersection, pms)
    }
    println(s"\t  Prepare Done")
    val fetched = Profiler("Fetch") {
      pms.map { pm => pm.queryIntersection -> dc.fetch2[Double](List(pm)) }
    }
    println(s"\t  BatchFetch Done")
    val initResult = Profiler("Solve") {
      fetched.foreach { case (bits, array) => solver.add(bits, array) }
      solver.constructMST()
      solver.solve()
    }
    println(s"\t  BatchSolve Done")
    val initError = SolverTools.error(trueResult, initResult)
    val prepareTime = Profiler.getDurationMicro("Prepare")
    def fetchTime = Profiler.getDurationMicro("Fetch")
    def solveTime = Profiler.getDurationMicro("Solve")
    def totalTime = prepareTime + fetchTime + solveTime
    val common = s"$dcname,${qu.mkString(":")},${qu.size},$algo,"
    fileout.println(common + s"0.0,$totalTime,$prepareTime,$fetchTime,$solveTime,$initError")

    val numWords = ((cuboid.size + 63) >> 6).toInt
    val numGroups = numWords >> groupSize
    val pi = new ProgressIndicator(numGroups,"\t Sampling")
    (0 to numGroups).foreach { i =>
      Profiler("Fetch") {
        (0 until 1 << groupSize).foreach { j =>
          val s = cuboid.projectFetch64((i << groupSize) + j, mask)
          solver.addSample(s)
        }
      }
      val result = Profiler("Solve") { solver.solve() }
      val error = SolverTools.error(trueResult, result)
      val fraction = (i + 1).toDouble / (numGroups + 1)
      fileout.println(common + s"$fraction,$totalTime,$prepareTime,$fetchTime,$solveTime,$error")
      pi.step
    }
  }
  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean=true, qname: String="", sliceValues: Seq[(Int, Int)]=Nil): Unit = {
    //runNaiveBatch(dc, dcname, qu, trueResult)
    //runMomentBatch(dc, dcname, qu, trueResult)
    //runIPFBatch(dc, dcname, qu, trueResult)
    //runIPF2Batch(dc, dcname, qu, trueResult)
    runNaiveOnline(14)(dc, dcname, qu, trueResult, sliceValues)
    //runMomentOnline(14, "V1")(dc, dcname, qu, trueResult)
    //runMomentOnline(14, "V2")(dc, dcname, qu, trueResult)
    //runMomentOnline(14, "V3")(dc, dcname, qu, trueResult)
    //runIPFOnline(14, "mix")(dc, dcname, qu, trueResult)
    //runIPFOnline(14, "nomix")(dc, dcname, qu, trueResult)
  }

}

object OnlineSamplingExperiment extends ExperimentRunner {
  def qsize(cg: CubeGenerator, isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val (minD, maxD) = cg match {
      case n: NYC => (18, 40)
      case s: SSB => (14, 30)
    }
    val logN = 15
    val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
    dc.loadPrimaryMoments(cg.baseName)
    val ename = s"${cg.inputname}-$isSMS-qsize"
    val expt = new OnlineSamplingExperiment(ename)
    //val mqr = new MaterializedQueryResult(cg, isSMS)  //for loading pre-generated queries and results
    Vector(10, 12, 14, 18).reverse.map { qs =>
      val queries = (0 until numIters).map
      { i => Tools.generateQuery(isSMS, cg.schemaInstance, qs) } //generate fresh queries
      //val queries = mqr.loadQueries(qs).take(numIters)
      queries.zipWithIndex.foreach { case (q, qidx) =>
        val trueResult = dc.naive_eval(q)
        //val trueResult = mqr.loadQueryResult(qs, qidx)
        expt.run(dc, dc.cubeName, q, trueResult)
      }
    }
    be.reset
  }
  def main(args: Array[String]): Unit = {
    implicit val be = CBackend.default
    val nyc = new NYC()
    val ssb = new SSB(100)

    def func(param: String)(timestamp: String, numIters: Int) = {
      implicit val ni = numIters
      implicit val ts = timestamp
      param match {
        case "qsize-nyc-prefix" => qsize(nyc, true)
        case "qsize-nyc-random" => qsize(nyc, false)

        case "qsize-ssb-prefix" => qsize(ssb, true)
        case "qsize-ssb-random" => qsize(ssb, false)
      }
    }

    run_expt(func)(args)
  }
}
