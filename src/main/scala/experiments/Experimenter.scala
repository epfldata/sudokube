package experiments

import backend.CBackend
import combinatorics.Combinatorics
import core._
import frontend.experiments.Tools
import frontend.generators.{MicroBench, NYC, SSB}

import java.io.PrintStream

object Experimenter {


  def cuboid_distribution(isSMS: Boolean) = {
    val ms = if (isSMS) "sms" else "rms"
    val cg = NYC
    val cubes = List(
      s"NYC_${ms}_14_10",
      s"NYC_${ms}_15_6",
      s"NYC_${ms}_15_10",
      s"NYC_${ms}_15_14",
      s"NYC_${ms}_16_10"
    )
    val maxD = 28
    val fileout = new PrintStream(s"expdata/Cuboids_${cg.inputname}_${ms}.csv")
    fileout.println("Name," + (0 to maxD).mkString(","))
    cubes.foreach { n =>
      val names = n.split("_")
      val logN = names(2).toInt
      val mod = names(3).toInt
      val dc = PartialDataCube.load2(n, cg.inputname + "_base")
      val projMap = dc.m.projections.groupBy(_.length).mapValues(_.length).withDefaultValue(0)
      val projs = (0 to maxD).map(i => projMap(i)).mkString(",")
      fileout.println(s"${logN}_${mod}," + projs)
      dc.cuboids.head.backend.reset
    }
    val sch = cg.schema()
    val total = if (isSMS) {
      sch.root.numPrefixUpto(maxD).map(_.toDouble).toList
    }
    else {
      (0 to maxD).map { i => Combinatorics.comb(sch.n_bits, i).toDouble }.toList
    }
    fileout.println(s"Total," + total.mkString(","))
  }

  def storage_overhead() = {
    def split(s: String) = "\\begin{tabular}{c}" + s + "\\end{tabular}"

    val fileout = new PrintStream(s"expdata/storage_overhead.tex")

    fileout.println(
      """
        |\begin{table}
        |\caption{Additional Storage Overhead}
        |\label{tab:overhead}
        |\begin{tabular}{|c|c|c|c|c|c|}
        |\hline
        |""".stripMargin +
        "Dataset & " + split("Base \\\\ Size") + "& $n$ & " + "$d_{\\min}$ & " + split("RMS \\\\ Ovrhd.") + "&" + split("SMS \\\\ Ovrhd.") + " \\\\ \n \\hline \n")
    val params = List(
      "NYC" -> "14_10",
      "NYC" -> "15_6",
      "NYC" -> "15_10",
      "NYC" -> "15_14",
      "NYC" -> "16_10",
      "SSB-sf100" -> "15_14")
    params.foreach { case (cgname, cubename) =>
      val names = cubename.split("_")
      val logN = names(0).toInt
      val mod = names(1).toInt
      val rmsname = cgname + "_rms_" + cubename
      val smsname = cgname + "_sms_" + cubename

      val dcrms = PartialDataCube.load2(rmsname, cgname + "_base")
      val basesize = dcrms.cuboids.last.numBytes
      val baseGB = Tools.round(basesize / math.pow(10, 9), 2)
      val rmsovrhead = Tools.round(dcrms.cuboids.map(_.numBytes).sum / basesize.toDouble - 1.0, 4)
      dcrms.cuboids.head.backend.reset

      val dcsms = PartialDataCube.load2(smsname, cgname + "_base")
      val smsvrhead = Tools.round(dcsms.cuboids.map(_.numBytes).sum / basesize.toDouble - 1.0, 4)
      dcsms.cuboids.head.backend.reset

      val (ds, bs) = if (cgname.startsWith("NYC")) {
        if (cubename.startsWith("14"))
          ("\\multirow{5}{*}{NYC}", s"\\multirow{5}{*}{$baseGB GB}")
        else ("", "")
      }
      else ("\\hline\nSSB", s"$baseGB GB")


      fileout.println(s"$ds & $bs & " + "$2^{" + logN + "}$" + s" & $mod & $rmsovrhead & $smsvrhead \\\\")

    }
    fileout.println(
      """\hline
        |\end{tabular}
        |\end{table}
        |""".stripMargin)
  }

  def lpp_query_dimensionality(isSMS: Boolean)(implicit shouldRecord: Boolean, numIters: Int) = {
    val cg = SSB(100)
    val param = "15_14"
    val ms = (if (isSMS) "sms" else "rms")
    val name = s"_${ms}_${param}"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load2(fullname, cg.inputname + "_base")
    val sch = cg.schema()
    val qss = List(6, 8, 10, 12)

    val queries = qss.flatMap { qs =>
      (0 until numIters).map(_ => sch.root.samplePrefix(qs)).distinct
    }

    val expname2 = s"query-dim-$ms"

    import RationalTools._
    val expt = new LPSolverBatchExpt[Rational](expname2)
    if (shouldRecord) expt.warmup()
    val ql = queries.length
    queries.zipWithIndex.foreach { case (q, i) =>
      println(s"Full Query $i/$ql")
      expt.run(dc, fullname, q, true)
    }
    dc.cuboids.head.backend.reset
  }


  def moment_query_dimensionality(isSMS: Boolean)(implicit shouldRecord: Boolean, numIters: Int): Unit = {
    val cg = SSB(100)
    val param = "15_14"
    val ms = (if (isSMS) "sms" else "rms")
    val name = s"_${ms}_${param}"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load2(fullname, cg.inputname + "_base")
    val sch = cg.schema()
    val qss = List(6, 9, 12, 15)
    val queries = qss.flatMap { qs =>
      (0 until numIters).map(_ => sch.root.samplePrefix(qs)).distinct
    }

    val expname2 = s"query-dim-$ms"
    val exptfull = new MomentSolverBatchExpt[Double](expname2)
    if (shouldRecord) exptfull.warmup()
    val ql = queries.length
    queries.zipWithIndex.foreach { case (q, i) =>
      println(s"Full Query $i/$ql")
      exptfull.run(dc, fullname, q)
    }

    val exptonline = new MomentSolverOnlineExpt[Double](expname2)
    if (shouldRecord) exptonline.warmup()
    queries.zipWithIndex.foreach { case (q, i) =>
      println(s"Online Query $i/$ql")
      exptonline.run(dc, fullname, q)
    }
    dc.cuboids.head.backend.reset
  }

  def moment_mat_params(isSMS: Boolean)(implicit shouldRecord: Boolean, numIters: Int) = {
    val cg = NYC
    val params = List(
      (14, 10),
      (15, 6), (15, 10), (15, 14),
      (16, 10)
    )
    val sch = cg.schema()

    val qs = 10
    val queries = (0 until numIters).map(_ => sch.root.samplePrefix(qs)).distinct
    val ms = (if (isSMS) "sms" else "rms")
    val expname2 = s"mat-params-$ms"
    val exptfull = new MomentSolverBatchExpt[Double](expname2)
    if (shouldRecord) exptfull.warmup()

    val exptonline = new MomentSolverOnlineExpt[Double](expname2)
    if (shouldRecord) exptonline.warmup()

    params.foreach { p =>
      val fullname = s"${cg.inputname}_${ms}_${p._1}_${p._2}"
      val dc = PartialDataCube.load2(fullname, cg.inputname + "_base")

      val ql = queries.length
      queries.zipWithIndex.foreach { case (q, i) =>
        println(s"Full Query $i/$ql")
        exptfull.run(dc, fullname, q)
      }

      queries.zipWithIndex.foreach { case (q, i) =>
        println(s"Online Query $i/$ql")
        exptonline.run(dc, fullname, q)
      }

      dc.cuboids.head.backend.reset
    }

  }

  def mb_dims()(implicit shouldRecord: Boolean, numIters: Int): Unit = {
    val expt = new MomentSolverOnlineExpt[Double]("mb-dims", true)
    if (shouldRecord) expt.warmup()
    List(6, 8, 10, 12).foreach { nb =>
      val cg = MicroBench(nb, 100000, 0.5, 0.25)
      val fullname = cg.inputname + "_all"

      (0 until numIters).foreach { i =>
        val (sch, r_its) = cg.generate2()
        sch.initBeforeEncode()
        val dc = new DataCube(MaterializationScheme.all_cuboids(cg.n_bits))
        dc.build(CBackend.b.mkParallel(sch.n_bits, r_its))

        val q = 0 until cg.n_bits
        expt.run(dc, fullname, q)
        dc.cuboids.head.backend.reset
      }
    }
  }


  def mb_total()(implicit shouldRecord: Boolean, numIters: Int): Unit = {
    val expt = new MomentSolverOnlineExpt[Double]("mb-total", true)
    if (shouldRecord) expt.warmup()
    List(2, 3, 4, 5).foreach { tot =>
      val cg = MicroBench(10, math.pow(10, tot).toInt, 0.5, 0.25)
      val fullname = cg.inputname + "_all"
      (0 until numIters).foreach { i =>
        val (sch, r_its) = cg.generate2()
        sch.initBeforeEncode()
        val dc = new DataCube(MaterializationScheme.all_cuboids(cg.n_bits))
        dc.build(CBackend.b.mkParallel(sch.n_bits, r_its))

        val q = 0 until cg.n_bits
        expt.run(dc, fullname, q)
        dc.cuboids.head.backend.reset
      }
    }
  }


  def mb_stddev()(implicit shouldRecord: Boolean, numIters: Int): Unit = {
    val expt = new MomentSolverOnlineExpt[Double]("mb-stddev", true)
    if (shouldRecord) expt.warmup()
    List(0.2, 0.4, 0.6, 0.8).foreach { stddev =>
      val cg = MicroBench(10, 100000, stddev, 0.25)
      val fullname = cg.inputname + "_all"
      (0 until numIters).foreach { i =>
        val (sch, r_its) = cg.generate2()
        sch.initBeforeEncode()
        val dc = new DataCube(MaterializationScheme.all_cuboids(cg.n_bits))
        dc.build(CBackend.b.mkParallel(sch.n_bits, r_its))

        val q = 0 until cg.n_bits
        expt.run(dc, fullname, q)
        dc.cuboids.head.backend.reset
      }
    }
  }

  def mb_prob()(implicit shouldRecord: Boolean, numIters: Int): Unit = {
    val expt = new MomentSolverOnlineExpt[Double]("mb-prob", true)
    if (shouldRecord) expt.warmup()
    List(0.1, 0.2, 0.3, 0.4).foreach { prob =>
      val cg = MicroBench(10, 100000, 0.5, prob)
      val fullname = cg.inputname + "_all"
      (0 until numIters).foreach { i =>
        val (sch, r_its) = cg.generate2()
        sch.initBeforeEncode()
        val dc = new DataCube(MaterializationScheme.all_cuboids(cg.n_bits))
        dc.build(CBackend.b.mkParallel(sch.n_bits, r_its))

        val q = 0 until cg.n_bits
        expt.run(dc, fullname, q)
        dc.cuboids.head.backend.reset
      }
    }
  }

  def debug(): Unit = {
    implicit val shouldRecord = false
    val cg = NYC
    val isSMS = false
    //val param = "15_28_0"
    //val name = (if (isSMS) "_sms_" else "_rms2_") + param
    //val fullname = cg.inputname + name
    //val dc = PartialDataCube.load2(fullname, cg.inputname + "_base")


    //val q1 = Vector(75, 134, 168, 178, 188, 219, 237, 276, 315, 355)
    //val q2 = List(116, 117, 118, 119, 120, 129, 130, 131, 137, 138, 139, 155, 172, 180, 192)
    val queries = (0 to 4).map(i => Tools.rand_q(429, 10))
    ////val numQs = sch.root.numPrefixUpto(15)
    ////(0 until 15).map(i => println(s"$i => " + numQs(i)))
    //

    //val expt = new UniformSolverFullExpt[Double](fullname)
    //import RationalTools._
    //val expt = new LPSolverFullExpt[Rational](dc, fullname)
    //expt.warmup(10)


    //val sample = Exponential
    //val cg = MBSimple(12)

    //val fullname = cg.inputname + "_all"
    //val dc = DataCube.load2(fullname)

    //val (sch, r_its) = cg.generate2()
    //sch.initBeforeEncode()
    //val dc = new DataCube(MaterializationScheme.all_cuboids(cg.n_bits))
    //dc.build(CBackend.b.mkParallel(sch.n_bits, r_its))

    //val q = 0 until 12
    //val res = dc.naive_eval(q)
    //val zeroes = res.filter(_ == 0.0).length
    //val sparse = zeroes.toDouble / res.length
    //val tot = res.sum
    //val naiveM = SolverTools.fastMoments(res).map(x => (x * 10000 / tot).toInt)
    //naiveM.zipWithIndex.sortBy(-_._1).take(15).foreach(println)
    //println("Sparsity = " + sparse)
    //val resMax = res.max.toInt
    //val step = math.max(resMax/1000,1)
    //res.groupBy(x => (x.toInt/step) * step).mapValues(_.length).toList.sortBy(_._1).foreach(println)
    val fullname = "NYC_sms_16_10"
    val dc = PartialDataCube.load2(fullname, "NYC_base")
    //val m2 =new EfficientMaterializationScheme(dc.m)
    val expt = new MomentSolverBatchExpt[Double](fullname)
    //val expt = new UniformSolverOnlineExpt[Double](fullname, true)
    queries.foreach { q1 => dc.m.prepare(q1, 50, 400) }
  }

  def main(args: Array[String]) {
    implicit val shouldRecord = true
    implicit val numIters = 100
    args.lift(0).getOrElse("debug") match {
      case "Fig7" =>
        cuboid_distribution(false)
        cuboid_distribution(true)
      case "Tab1" => storage_overhead()
      case "Fig8" =>
        lpp_query_dimensionality(false)
        lpp_query_dimensionality(true)
      case "Fig9" =>
        moment_query_dimensionality(false)
        moment_query_dimensionality(true)
      case "Fig10" =>
        moment_mat_params(false)
        moment_mat_params(true)
      case "Fig11" =>
        mb_dims()
        mb_stddev()
        mb_prob()
      case _ => debug()
    }
  }
}
