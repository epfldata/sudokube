package experiments

import backend.CBackend
import combinatorics.Combinatorics
import core._
import frontend.experiments.Tools
import frontend.generators.{MicroBench, NYC, SSB}

import java.io.PrintStream

object Experimenter {


  def multi_storage(isSMS: Boolean) = {
    val ms = if (isSMS) "sms" else "rms2"
    val cg = NYC
    val cubes = List(
      s"NYC_${ms}_14_23_0",
      s"NYC_${ms}_15_20_0",
      s"NYC_${ms}_15_24_0",
      s"NYC_${ms}_15_28_0",
      s"NYC_${ms}_16_25_0"
    )
    val maxD = 28
    val fileout = new PrintStream(s"expdata/MultiStorage_${cg.inputname}_${ms}.csv")
    fileout.println("Name," + (0 to maxD).mkString(","))
    cubes.foreach { n =>
      val names = n.split("_")
      val logN = names(2).toInt
      val mod = names(4).toInt + names(3).toInt + 1 - logN
      val modstr = String.format("%02d", Int.box(mod))
      val dc = PartialDataCube.load2(n, cg.inputname + "_base")
      val projMap = dc.m.projections.groupBy(_.length).mapValues(_.length).withDefaultValue(0)
      val projs = (0 to maxD).map(i => projMap(i)).mkString(",")
      //println("MAX D =" + dc.m.projections.map(_.length).max)
      fileout.println(s"${logN}_${modstr}," + projs)
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

  def cubestats() = {
    def split(s: String) = "\\begin{tabular}{c}" + s + "\\end{tabular}"

    val fileout = new PrintStream(s"expdata/cubestats.tex")

    fileout.println(
      """
        |\begin{figure}
        |\begin{tabular}{|c|c|c|c|c|c|}
        |\hline
        |""".stripMargin +
        "Dataset & " + split("Base \\\\ Size") + "& $n$ & " + "$d_{\\min}$ & " + split("RMS \\\\ Ovrhd.") + "&" + split("SMS \\\\ Ovrhd.") + " \\\\ \n \\hline \n")
    val params = List(
      "NYC" -> "14_23_0",
      "NYC" -> "15_20_0",
      "NYC" -> "15_24_0",
      "NYC" -> "15_28_0",
      "NYC" -> "16_25_0",
      "SSB-sf100" -> "15_28_0")
    params.foreach { case (cgname, cubename) =>
      val names = cubename.split("_")
      val logN = names(0).toInt
      val mod = names(1).toInt + 1 - logN
      val rmsname = cgname + "_rms2_" + cubename
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


      fileout.println(s"$ds & $bs & " + "$2^{" + logN +"}$" +s" & $mod & $rmsovrhead & $smsvrhead \\\\")

    }
    fileout.println(
      """\hline
        |\end{tabular}
        |\caption{Additional Storage Overhead}
        |\label{fig:overhead}
        |\end{figure}
        |""".stripMargin)
  }

  def lpp_qsize(isSMS: Boolean)(implicit shouldRecord: Boolean) = {
    val cg = SSB(100)
    val param = "15_28_0"
    val ms = (if (isSMS) "sms" else "rms2")
    val name = s"_${ms}_${param}"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load2(fullname, cg.inputname + "_base")
    val sch = cg.schema()
    val qss = if (isSMS) List(6, 8, 10, 12) else List(6, 8, 10, 11)
    val nq = 100
    val queries = qss.flatMap { qs =>
      (0 until nq).map(_ => sch.root.samplePrefix(qs)).distinct
    }

    val expname2 = fullname + "_qs"

    import RationalTools._
    val expt = new LPSolverFullExpt[Rational](expname2)
    if (shouldRecord) expt.warmup()
    queries.foreach(q => expt.run(dc, fullname, q, true))
    dc.cuboids.head.backend.reset
  }


  def uniform_qsize(isSMS: Boolean)(implicit shouldRecord: Boolean): Unit = {
    val cg = SSB(100)
    val param = "15_28_0"
    val ms = (if (isSMS) "sms" else "rms2")
    val name = s"_${ms}_${param}"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load2(fullname, cg.inputname + "_base")
    val sch = cg.schema()
    val qss = List(6, 9, 12, 15, 18)
    val nq = 100
    val queries = qss.flatMap { qs =>
      (0 until nq).map(_ => sch.root.samplePrefix(qs)).distinct
    }

    val expname2 = fullname + "_qs"
    val exptfull = new UniformSolverFullExpt[Double](expname2)
    if (shouldRecord) exptfull.warmup()
    val ql = queries.length
    queries.zipWithIndex.foreach { case (q, i) =>
      println(s"Full Query $i/$ql")
      exptfull.run(dc, fullname, q)
    }

    val exptonline = new UniformSolverOnlineExpt[Double](expname2)
    if (shouldRecord) exptonline.warmup()
    queries.zipWithIndex.foreach { case (q, i) =>
      println(s"Online Query $i/$ql")
      exptonline.run(dc, fullname, q)
    }
    dc.cuboids.head.backend.reset
  }

  def uniform_cubes(isSMS: Boolean)(implicit shouldRecord: Boolean) = {
    val cg = NYC
    val params = List(
      (14, 23),
      (15, 20), (15, 24), (15, 28),
      (16, 25)
    )
    val sch = cg.schema()
    val nq = 100
    val qs = 10
    val queries = (0 until nq).map(_ => sch.root.samplePrefix(qs)).distinct
    val ms = (if (isSMS) "sms" else "rms2")
    val expname2 = s"NYC_${ms}_cubes"
    val exptfull = new UniformSolverFullExpt[Double](expname2)
    if (shouldRecord) exptfull.warmup()

    val exptonline = new UniformSolverOnlineExpt[Double](expname2)
    if (shouldRecord) exptonline.warmup()

    params.foreach { p =>
      val fullname = s"${cg.inputname}_${ms}_${p._1}_${p._2}_0"
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

  def mb2(): Unit = {
    implicit val shouldRecord = true
    val cg = NYC
    val sch = cg.schema()


    val queries =  {
      val q1 = Vector(75, 134, 168, 178, 188, 219, 237, 276, 315, 355, 393, 428)
      val q2 = Vector(75, 168, 188, 237, 315, 393)
      List(q1, q2)
    }
    queries.foreach{ q =>
      val mbcname = s"NYC_${q.size}D_allsubsets"
      //val dcw = new PartialDataCube(MaterializationScheme.all_subsetsOf(sch.n_bits, q), cg.inputname + "_base")
      //dcw.build()
      //dcw.save2(mbcname)
      val expt = new UniformSolverOnlineExpt[Double](s"mb-test-${q.size}D")
      val names = List(mbcname, "NYC_sms_15_28_0", "NYC_rms2_15_28_0")
      names.foreach{fulln =>
        val dc = PartialDataCube.load2(fulln, "NYC_base")
        expt.run(dc, fulln, q, true)
        dc.cuboids.head.backend.reset
      }
    }
  }

  def mb_dims()(implicit shouldRecord: Boolean): Unit = {
    val expt = new UniformSolverOnlineExpt[Double]("mb-dims", true)
    if (shouldRecord) expt.warmup()
    List(6, 8, 10, 12).foreach { nb =>
      val cg = MicroBench(nb, 100000, 0.5, 0.25)
      val fullname = cg.inputname + "_all"
      val num_iters = 30
      (0 until num_iters).foreach { i =>
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


  def mb_total()(implicit shouldRecord: Boolean): Unit = {
    val expt = new UniformSolverOnlineExpt[Double]("mb-total", true)
    if (shouldRecord) expt.warmup()
    List(2, 3, 4, 5).foreach { tot =>
      val cg = MicroBench(10, math.pow(10, tot).toInt, 0.5, 0.25)
      val fullname = cg.inputname + "_all"
      val num_iters = 30
      (0 until num_iters).foreach { i =>
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


  def mb_stddev()(implicit shouldRecord: Boolean): Unit = {
    val expt = new UniformSolverOnlineExpt[Double]("mb-stddev", true)
    if (shouldRecord) expt.warmup()
    List(0.2, 0.4, 0.6, 0.8).foreach { stddev =>
      val cg = MicroBench(10, 100000, stddev, 0.25)
      val fullname = cg.inputname + "_all"
      val num_iters = 30
      (0 until num_iters).foreach { i =>
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

  def mb_prob()(implicit shouldRecord: Boolean): Unit = {
    val expt = new UniformSolverOnlineExpt[Double]("mb-prob", true)
    if (shouldRecord) expt.warmup()
    List(0.1, 0.2, 0.3, 0.4).foreach { prob =>
      val cg = MicroBench(10, 100000, 0.5, prob)
      val fullname = cg.inputname + "_all"
      val num_iters = 30
      (0 until num_iters).foreach { i =>
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


    val q1 = Vector(75, 134, 168, 178, 188, 219, 237, 276, 315, 355, 393, 428)
    //val q2 = List(116, 117, 118, 119, 120, 129, 130, 131, 137, 138, 139, 155, 172, 180, 192)
    val queries = List(q1)
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

    //val expt = new UniformSolverOnlineExpt[Double](fullname, true)
    //expt.run(dc, fullname, q)
  }

  def main(args: Array[String]) {
    implicit val shouldRecord = true

    args.lift(0).getOrElse("debug") match {
      case "Fig7" =>
        multi_storage(false)
        multi_storage(true)
      case "Tab1" =>  cubestats()
      case "Fig8" =>
        lpp_qsize(false)
        lpp_qsize(true)
      case "Fig9" =>
        uniform_qsize(false)
        uniform_qsize(true)
      case "Fig10" =>
        uniform_cubes(false)
        uniform_cubes(true)
      case "Fig11" =>
        mb_dims()
        mb_stddev()
        mb_prob()
      case _ => debug()
    }
  }
}
