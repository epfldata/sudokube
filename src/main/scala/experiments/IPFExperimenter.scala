package experiments

import backend.CBackend
import core.{MaterializedQueryResult, PartialDataCube}
import frontend.generators.{AirlineDelay, CubeGenerator, NYC, SSB}
import frontend.schema.encoders.ColEncoder

import java.io.{File, PrintStream}
import java.time.format.DateTimeFormatter

/**
 * Compare the time and error for IPF solvers and moment solver.
 * @author Zhekai Jiang
 */
object IPFExperimenter {
  implicit var backend: CBackend = CBackend.colstore
  var backendName = "colstore"

  def cuboid_stats(isSMS: Boolean, cg: CubeGenerator) = {
    val maxD = 40
    val params = List(
      (15, 6), (15, 10), (15, 14),
      (15, 18),
      (12, 18), (9, 18), (6, 18))
    val ms = if (isSMS) "sms3" else "rms3"
    val dc = cg.loadBase()
    val nbits = dc.index.n_bits
    val baseTotal = dc.cuboids.head.numBytes
    backend.reset
    val file = new File(s"expdata/ipf-expts/latest/${cg.inputname}-$ms-cuboids.csv")
    if (!file.exists())
      file.getParentFile.mkdirs()
    val fileout = new PrintStream(file)
    fileout.println("Dataset,MS,logn,dmin,maxD,TotalBytes,Ratio")
    params.foreach { case (logn, dmin) =>
      val cubeName = s"${cg.inputname}_${ms}_${logn}_${dmin}_$maxD"
      println(s"Getting size of datacube  $cubeName")
      val dc = PartialDataCube.load(cubeName, cg.baseName)
      val bytes = dc.cuboids.filter(_.n_bits < nbits).map(_.numBytes).sum
      val ratio = bytes.toDouble / baseTotal
      fileout.println(s"${cg.inputname},${ms},${logn},${dmin},${maxD},$bytes,$ratio")
      backend.reset
    }
  }

  def manual_online(cubeGenerator: String)(implicit timestampedFolder: String): Unit = {
    val (query, qName, cg) = if (cubeGenerator == "NYC") {
      val cg: CubeGenerator = NYC()
      val sch = cg.schemaInstance
      val encMap = sch.columnVector.map(c => c.name -> c.encoder).toMap[String, ColEncoder[_]]
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
    val expt = new IPFMomentOnlineExpt(cg.inputname)
    val trueResult = dc.naive_eval(query)
    expt.run(dc, dc.cubeName, query, trueResult, qname = qName, sliceValues = Nil)
  }


  def ipf_moment_expt_qsize(isSMS: Boolean, cubeGenerator: String, logn: Int, minDim: Int, maxDim: Int)(implicit numIters: Int, timestampedFolder: String) = {
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
    expt.warmup()
    //val materializedQueries = new MaterializedQueryResult(cg)
    val qss = List(8, 10, 12, 14, 16) //, 18)
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

  def ipf_moment_expt_logn(isSMS: Boolean, cubeGenerator: String, qs: Int)(implicit numIters: Int, timestampedFolder: String) = {
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
    expt.warmup()

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

  def ipf_moment_expt_dmin(isSMS: Boolean, cubeGenerator: String, qs: Int)(implicit numIters: Int, timestampedFolder: String) = {
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
    expt.warmup()

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

  def ipf_moment_partial(isSMS: Boolean, cubeGenerator: String, qs: Int, logn: Int, minDim: Int, maxDim: Int)(implicit numIters: Int, timestampedFolder: String): Unit = {
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

    val expname2 = s"$cubeGenerator-$ms-qdim-$qs-dmin-$minDim-logn-$logn-$backendName"
    val expt = new IPFMomentBatchExpt3(expname2)
    expt.warmup()
    println(s"Running Moment IPF Partial Batch Experiment for $cubeGenerator dataset MS=$ms QueryDimensionality=$qs NumCuboids=2^{$logn} MinDim=${minDim}")
    val queries = (0 until numIters).map(_ => sch.root.samplePrefix(qs)).distinct
    val ql = queries.length
    queries.zipWithIndex.foreach { case (q, i) =>
      println(s"\tBatch Query ${i + 1}/$ql")
      //val trueResult = materializedQueries.loadQueryResult(qs, i)
      val trueResult = dc.naive_eval(q.sorted)
      expt.run(dc, fullname, q, trueResult, sliceValues = Vector())
    }
  }

  def ipf_moment_compareTimeError(isSMS: Boolean, cubeGenerator: String, minNumDimensions: Int)(implicit numIters: Int, timestampedFolder: String): Unit = {
    val cg: CubeGenerator = if (cubeGenerator == "NYC") NYC() else SSB(100)
    val param = s"15_${minNumDimensions}_30"
    val ms = if (isSMS) "sms3" else "rms3"
    val name = s"_${ms}_$param"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load(fullname, cg.baseName)
    dc.loadPrimaryMoments(cg.baseName)

    val expname2 = s"query-dim-$cubeGenerator-$ms-dmin-$minNumDimensions-$backendName"
    val exptfull = new IPFMomentBatchExpt(expname2)
    exptfull.warmup()
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

  def error_analysis()(implicit timestampedFolder: String): Unit = {
    val cg = NYC()
    val sch = cg.schemaInstance
    //val query = sch.root.samplePrefix(15).sorted
    val query = Vector(37,49,50,51,52,53,54,55,138,139,219,365,366,404,405)
    val param = s"15_18_40"
    val ms = if (true) "sms3" else "rms3"
    val name = s"_${ms}_$param"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load(fullname, cg.baseName)
    assert(dc.index.n_bits == sch.n_bits)
    dc.loadPrimaryMoments(cg.baseName)

    val expt = new ErrorAnalysis(cg.inputname)
    val trueResult = dc.naive_eval(query)
    expt.run(dc, dc.cubeName, query, trueResult)
  }

  def main(args: Array[String]): Unit = {
    implicit val numIters: Int = 100
    val maxdim = 40
    val partialQS = 9
    val nyc = new NYC()
    val airline = new AirlineDelay()
    implicit val timestampedFolder = args.lift(1).getOrElse(Experiment.now())
    args.lift(0).getOrElse("default") match {
      //case "default" => ()


      case "NYC-SMS-qsize" => ipf_moment_expt_qsize(true, "NYC", 15, 18, maxdim)
      case "NYC-SMS-logn" => ipf_moment_expt_logn(true, "NYC", 15)
      case "NYC-SMS-dmin" => ipf_moment_expt_dmin(true, "NYC", 15)
      case "NYC-SMS-partial" => ipf_moment_partial(true, "NYC", partialQS, 15, 18, maxdim)
      case "NYC-SMS-stats" => cuboid_stats(true, nyc)

      case "NYC-RMS-qsize" => ipf_moment_expt_qsize(false, "NYC", 15, 18, maxdim)
      case "NYC-RMS-logn" => ipf_moment_expt_logn(false, "NYC", 15)
      case "NYC-RMS-dmin" => ipf_moment_expt_dmin(false, "NYC", 15)
      case "NYC-RMS-partial" => ipf_moment_partial(false, "NYC", partialQS, 15, 18, maxdim)
      case "NYC-RMS-stats" => cuboid_stats(false, nyc)

      case "NYC-online" => manual_online("NYC")

      case "Airline-SMS-qsize" => ipf_moment_expt_qsize(true, "Airline", 15, 18, maxdim)
      case "Airline-SMS-logn" => ipf_moment_expt_logn(true, "Airline", 15)
      case "Airline-SMS-dmin" => ipf_moment_expt_dmin(true, "Airline", 15)
      case "Airline-SMS-partial" => ipf_moment_partial(true, "Airline", partialQS, 15, 18, maxdim)
      case "Airline-SMS-stats" => cuboid_stats(true, airline)

      case "Airline-RMS-qsize" => ipf_moment_expt_qsize(false, "Airline", 15, 18, maxdim)
      case "Airline-RMS-logn" => ipf_moment_expt_logn(false, "Airline", 15)
      case "Airline-RMS-dmin" => ipf_moment_expt_dmin(false, "Airline", 15)
      case "Airline-RMS-partial" => ipf_moment_partial(false, "Airline", partialQS, 15, 18, maxdim)
      case "Airline-RMS-stats" => cuboid_stats(false, airline)

      case "Airline-online" => manual_online("Airline")
      case "error" => error_analysis()
      //case s => println(s"Unknown Expt $s with timestamp $timestampedFolder")

    }

  }
}
