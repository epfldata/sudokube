package experiments

import backend.CBackend
import core.solver.SolverTools
import core.solver.SolverTools.{entropy, entropyBase2, error, normalizedEntropyBase2}
import core.solver.iterativeProportionalFittingSolver.{EffectiveIPFSolver, IPFUtils}
import core.solver.moment.{CoMoment5SolverDouble, Moment1TransformerDouble}
import core.{MaterializedQueryResult, PartialDataCube}
import frontend.generators.{AirlineDelay, CubeGenerator, NYC, SSB}
import frontend.schema.encoders.{ColEncoder, LazyMemCol, StaticDateCol, StaticNatCol}
import util.{BitUtils, Profiler, ProgressIndicator}

import java.io.{File, PrintStream}

/**
 * Compare the time and error for IPF solvers and moment solver.
 * @author Zhekai Jiang
 */
object IPFExperimenter {
  implicit var backend: CBackend = CBackend.colstore
  var backendName = "colstore"

  def manual_online(cubeGenerator: String): Unit = {
    val (query, qName, cg) = if (cubeGenerator == "NYC") {
      val cg: CubeGenerator = NYC()
      val sch = cg.schemaInstance
      val encMap = sch.columnVector.map(c => c.name -> c.encoder).toMap[String, ColEncoder[_]]

      //val year = encMap("Issue Date").asInstanceOf[StaticDateCol].yearCol.bits
      //val month = encMap("Issue Date").asInstanceOf[StaticDateCol].monthCol.bits
      val registrState = encMap("Registration State").bits
      val lawSection = encMap("Law Section").bits
      val (queryCols, qName) = Vector(registrState, lawSection) -> "registration_state;law_section"
      val query = queryCols.reduce(_ ++ _).sorted

      (query, qName, cg)
    } else {
      val cg: CubeGenerator = new AirlineDelay()
      val sch = cg.schemaInstance
      val encMap = sch.columnVector.map(c => c.name -> c.encoder).toMap[String, ColEncoder[_]]

      val state = encMap("OriginState").bits
      val dayofweek = encMap("DayOfWeek").bits
      val distanceGroup = encMap("DistanceGroup").bits.drop(2)

      val (queryCols, qName) = Vector(state, dayofweek, distanceGroup) -> "state;day_of_week;distance_group/4"
      val query = queryCols.reduce(_ ++ _).sorted
      (query, qName, cg)
    }
    val dc = cg.loadBase()
    dc.loadPrimaryMoments(cg.baseName)
    val primaryMoments = SolverTools.preparePrimaryMomentsForQuery[Double](query, dc.primaryMoments)
    val total = primaryMoments.head._2
    assert(primaryMoments.head._1 == 0)
    val pmMap = primaryMoments.map { case (i, m) => i -> m / total }.toMap
    val trueResult = dc.naive_eval(query)
    println(s"Online experiment for $cubeGenerator Query size = ${query.size}")
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

    def onlineExpt(name: String, data: Seq[(Int, Int, Double, Double, Double, Array[Double])]) = {
      val pi = new ProgressIndicator(data.size, s"Running experiment ordered by $name")
      val file = new File(s"expdata/ipf-expts/$cubeGenerator-$name-online.csv")

      if (!file.exists())
        file.getParentFile.mkdirs()

      val onlineFileStream = new PrintStream(file)
      onlineFileStream.println("QueryName,QuerySize,FetchID,CuboidID,CuboidDim,Mu,Entropy,NormalizedEntropy,IPFError,CM5Error,IPFSolveTime,CM5SolveTime,IPFIterations")
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
        onlineFileStream.println(s"$qName,${query.length},$idx,$i,$si,$mu,$e,$ne,$ipfError,$cm5Error,$ipfSolveTime,$cm5SolveTime,$ipfiters")
        pi.step
      }
      onlineFileStream.close()
    }

    onlineExpt("cuboidsize", alldata.sortBy { case (i, si, mu, e, ne, marginal) => si })
    onlineExpt("muabs", alldata.sortBy { case (i, si, mu, e, ne, marginal) => -math.abs(mu) })
    onlineExpt("entropy", alldata.sortBy { case (i, si, mu, e, ne, marginal) => e })

  }


  def ipf_moment_expt_qsize(isSMS: Boolean, cubeGenerator: String, logn: Int, minDim: Int, maxDim: Int)(implicit shouldRecord: Boolean, numIters: Int) = {
    val cg = if (cubeGenerator == "NYC") {
      NYC()
    } else {
      new AirlineDelay()
    }
    val sch = cg.schemaInstance
    val param = s"${logn}_${minDim}_${maxDim}"
    val ms = if (isSMS) "sms3" else "rms3"
    val name = s"_${ms}_$param"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load(fullname, cg.baseName)
    assert(dc.index.n_bits == sch.n_bits)
    dc.loadPrimaryMoments(cg.baseName)

    val expname2 = s"query-dim-$cubeGenerator-$ms-dmin-$minDim-$backendName"
    val expt = new IPFMomentBatchExpt2(expname2)
    if (shouldRecord) expt.warmup()
    //val materializedQueries = new MaterializedQueryResult(cg)
    val qss = List(8, 10, 12, 14, 16, 18)
    qss.foreach { qs =>
      //val queries = materializedQueries.loadQueries(qs).take(numIters)
      val queries = (0 until numIters).map(_ => sch.root.samplePrefix(qs)).distinct
      println(s"IPF Moment Stats Experiment for ${cg.inputname} dataset MS = $ms (d_min = $minDim) Query Dimensionality = $qs")
      val ql = queries.length
      queries.zipWithIndex.foreach { case (q, i) =>
        println(s"\tBatch Query ${i + 1}/$ql")
        //val trueResult = materializedQueries.loadQueryResult(qs, i)
        val trueResult = dc.naive_eval(q.sorted)
        expt.run(dc, fullname, q, trueResult, sliceValues = Vector())
      }
    }
    backend.reset
  }

  def ipf_moment_expt_logn(isSMS: Boolean, cubeGenerator: String, qs: Int)(implicit shouldRecord: Boolean, numIters: Int) = {
    backendName = "colstore"
    backend = CBackend.colstore
    val cg = if (cubeGenerator == "NYC") {
      NYC()
    } else {
      new AirlineDelay()
    }
    val sch = cg.schemaInstance

    val ms = if (isSMS) "sms3" else "rms3"
    val maxD = 40
    val dmin = 18
    val params = List(15, 12, 9, 6)
    //val materializedQueries = new MaterializedQueryResult(cg)
    //val queries = materializedQueries.loadQueries(qs).take(numIters)
    val queries = (0 until numIters).map(_ => sch.root.samplePrefix(qs)).distinct
    val expname2 = s"query-logn-$cubeGenerator-$ms-qsize-$qs-$backendName"
    val expt = new IPFMomentBatchExpt2(expname2)
    if (shouldRecord) expt.warmup()

    params.foreach { logn =>
      val fullname = s"${cg.inputname}_${ms}_${logn}_${dmin}_$maxD"
      val dc = PartialDataCube.load(fullname, cg.baseName)
      dc.loadPrimaryMoments(cg.inputname + "_base")
      assert(dc.index.n_bits == sch.n_bits)
      println(s"IPF Moment Stats Experiment for ${cg.inputname} dataset MS = $ms Query Dimensionality = $qs NumCuboids=2^{${logn}}, dmin=${dmin}")
      val ql = queries.length
      queries.zipWithIndex.foreach { case (q, i) =>
        println(s"\tBatch Query ${i + 1}/$ql")
        //val trueResult = materializedQueries.loadQueryResult(qs, i)
        val trueResult = dc.naive_eval(q.sorted)
        expt.run(dc, fullname, q, trueResult, sliceValues = Vector())
      }
      backend.reset
    }
  }

  def ipf_moment_expt_dmin(isSMS: Boolean, cubeGenerator: String, qs: Int)(implicit shouldRecord: Boolean, numIters: Int) = {
    backendName = "colstore"
    backend = CBackend.colstore
    val cg = if (cubeGenerator == "NYC") {
      NYC()
    } else {
      new AirlineDelay()
    }
    val sch = cg.schemaInstance

    val ms = if (isSMS) "sms3" else "rms3"
    val maxD = 40
    val logn = 15
    val params = List(18, 14, 10, 6)
    //val materializedQueries = new MaterializedQueryResult(cg)
    //val queries = materializedQueries.loadQueries(qs).take(numIters)
    val queries = (0 until numIters).map(_ => sch.root.samplePrefix(qs)).distinct
    val expname2 = s"query-dmin-$cubeGenerator-$ms-qsize-$qs-$backendName"
    val expt = new IPFMomentBatchExpt2(expname2)
    if (shouldRecord) expt.warmup()

    params.foreach { dmin =>
      val fullname = s"${cg.inputname}_${ms}_${logn}_${dmin}_$maxD"
      val dc = PartialDataCube.load(fullname, cg.baseName)
      dc.loadPrimaryMoments(cg.inputname + "_base")
      assert(dc.index.n_bits == sch.n_bits)
      println(s"IPF Moment Stats Experiment for ${cg.inputname} dataset MS = $ms Query Dimensionality = $qs NumCuboids=2^{${logn}}, dmin=${dmin}")
      val ql = queries.length
      queries.zipWithIndex.foreach { case (q, i) =>
        println(s"\tBatch Query ${i + 1}/$ql")
        //val trueResult = materializedQueries.loadQueryResult(qs, i)
        val trueResult = dc.naive_eval(q.sorted)
        expt.run(dc, fullname, q, trueResult, sliceValues = Vector())
      }
      backend.reset
    }
  }

  def ipf_moment_compareTimeError(isSMS: Boolean, cubeGenerator: String, minNumDimensions: Int)(implicit shouldRecord: Boolean, numIters: Int): Unit = {
    val cg: CubeGenerator = if (cubeGenerator == "NYC") NYC() else SSB(100)
    val param = s"15_${minNumDimensions}_30"
    val ms = if (isSMS) "sms3" else "rms3"
    val name = s"_${ms}_$param"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load(fullname, cg.baseName)
    dc.loadPrimaryMoments(cg.baseName)

    val expname2 = s"query-dim-$cubeGenerator-$ms-dmin-$minNumDimensions-$backendName"
    val exptfull = new IPFMomentBatchExpt(expname2)
    if (shouldRecord) exptfull.warmup()
    val materializedQueries = new MaterializedQueryResult(cg)
    val qss = List(6, 9, 12, 15, 18, 21, 24)
    qss.foreach { qs =>
      val queries = materializedQueries.loadQueries(qs).take(numIters)
      println(s"IPF Solvers vs Moment Solver Experiment for ${cg.inputname} dataset MS = $ms (d_min = $minNumDimensions) Query Dimensionality = $qs")
      val ql = queries.length
      queries.zipWithIndex.foreach { case (q, i) =>
        println(s"\tBatch Query ${i + 1}/$ql")
        val trueResult = materializedQueries.loadQueryResult(qs, i)
        exptfull.run(dc, fullname, q, trueResult, sliceValues = Vector())
      }
    }
    dc.cuboids.head.backend.reset
  }

  def main(args: Array[String]): Unit = {
    implicit val shouldRecord: Boolean = true
    implicit val numIters: Int = 20

    val maxdim = 40
    //ipf_moment_expt_qsize(true, "NYC", 15, 18, maxdim)
    //ipf_moment_expt_logn(true, "NYC", 15)
    //ipf_moment_expt_dmin(true, "NYC", 15)
    //
    //ipf_moment_expt_qsize(false, "NYC", 15, 18, maxdim)
    //ipf_moment_expt_logn(false, "NYC", 15)
    //ipf_moment_expt_dmin(false, "NYC", 15)
    //
    manual_online("NYC")


    //ipf_moment_expt_qsize(true, "Airline", 15, 18, maxdim)
    //ipf_moment_expt_logn(true, "Airline", 15)
    //ipf_moment_expt_dmin(true, "Airline", 15)
    //
    //ipf_moment_expt_qsize(false, "Airline", 15, 18, maxdim)
    //ipf_moment_expt_logn(false, "Airline", 15)
    //ipf_moment_expt_dmin(false, "Airline", 15)

    manual_online("Airline")


    //ipf_moment_compareTimeError(isSMS = true, cubeGenerator = "SSB", minNumDimensions = 14)
    //ipf_moment_compareTimeError(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 14)
    //ipf_moment_compareTimeError(isSMS = true, cubeGenerator = "NYC", minNumDimensions = 6)
    //ipf_moment_compareTimeError(isSMS = false, cubeGenerator = "SSB", minNumDimensions = 14)
    //ipf_moment_compareTimeError(isSMS = false, cubeGenerator = "NYC", minNumDimensions = 14)
    //ipf_moment_compareTimeError(isSMS = false, cubeGenerator = "NYC", minNumDimensions = 6)
  }
}
