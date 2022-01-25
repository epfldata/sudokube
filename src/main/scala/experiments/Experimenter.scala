package experiments

import backend.CBackend
import combinatorics.Combinatorics
import core._
import experiments.Experimenter.queryDistribution
import frontend.experiments.Tools
import frontend.generators.{CubeGenerator, Iowa, MicroBench, NYC, SSB}
import frontend.generators.MB_Sampler._
import frontend.schema.StructuredDynamicSchema
import util.{Profiler, Util}

import java.io.PrintStream
import java.time.Instant
import scala.util.Random

object Experimenter {



  def multi_storage(isSMS: Boolean) = {
    val ms = if (isSMS) "sms" else "rms2"
    val cg = NYC
    val cubes = List(
      s"NYC_${ms}_14_19_0",
      s"NYC_${ms}_14_25_2",
      s"NYC_${ms}_16_21_0",
      s"NYC_${ms}_16_25_4"
    )
    val maxD = 25
   val fileout = new PrintStream(s"expdata/MultiStorage_${cg.inputname}_${ms}_$maxD.csv")
   fileout.println("Name," + (0 to maxD).mkString(","))
    cubes.foreach { n =>
      val names = n.split("_")
      val logN = names(2).toInt
      val mod = names(4).toInt + names(3).toInt + 1 - logN
      val modstr = String.format("%02d",Int.box(mod))
      val dc = PartialDataCube.load2(n, cg.inputname + "_base")
      val projMap = dc.m.projections.groupBy(_.length).mapValues(_.length).withDefaultValue(0)
      val projs = (0 to maxD).map(i => projMap(i)).mkString(",")
      println("MAX D =" + dc.m.projections.map(_.length).max)
      fileout.println(s"${logN}_${modstr}," + projs)
    }
    val sch = cg.schema()
    val total = if(isSMS) {
       sch.root.numPrefixUpto(maxD).map(_.toDouble).toList
    }
    else {
      (0 to maxD).map{i => Combinatorics.comb(sch.n_bits, i).toDouble}.toList
    }
    fileout.println(s"Total,"+total.mkString(","))
  }

  def cubestats1() = {
    val fileout = new PrintStream(s"expdata/cubestats1.txt")
    fileout.println(
      """
        |\begin{tabular}{|c|c|c|}
        |\hline
        |logn & $d_{min}$ & Extra \\ \hline""".stripMargin)
    val cubes = List(
      "NYC_rms2_14_19_0",
      "NYC_rms2_14_23_0",
      "NYC_rms2_14_27_0",
      "NYC_rms2_15_20_0",
      "NYC_rms2_15_24_0",
      "NYC_rms2_15_28_0",
      "NYC_rms2_16_21_0",
      "NYC_rms2_16_25_0",
      "NYC_rms2_16_29_0"
    )
    cubes.foreach { n =>
      val names = n.split("_")
      val cgname = names(0)
      val logN = names(2).toInt
      val mod = names(4).toInt + names(3).toInt + 1 - logN
      val dc = PartialDataCube.load2(n, cgname + "_base")

      val basesize = dc.cuboids.last.numBytes
      val overhead = dc.cuboids.map(_.numBytes).sum / basesize.toDouble - 1.0

      fileout.println(s"$logN & $mod & ${Tools.round(overhead,4)} \\\\")

    }
    fileout.println(
      """\hline
        |\end{tabular}
        |""".stripMargin)
  }

  def cubestats2() = {
    val fileout = new PrintStream(s"expdata/cubestats2.txt")
    fileout.println(
      """
        |\begin{tabular}{|c|c|c|}
        |\hline
        |Dataset & Base & Extra \\ \hline
        |""".stripMargin)

    val cubenames = List(
      "NYC_rms2_15_20_0", //TODO Replace with 15_22
      "SSB-sf100_rms2_15_22_0",
      "SSB-sf10_rms2_15_22_0",
      "SSB-sf1_rms2_15_22_0"
    )
    cubenames.foreach { n =>
      val names = n.split("_")
      val cgname = names(0)
      val dc = PartialDataCube.load2(n, cgname + "_base")

      val basesize = dc.cuboids.last.numBytes

      val overhead0 = (dc.cuboids.map(_.numBytes).sum / basesize.toDouble - 1.0)
      val overhead = if(cgname.startsWith("NYC")) overhead0 * 4 else overhead0 //TODO: Remove HACK
      val baseGB = basesize/math.pow(10, 9)
      fileout.println(s"$cgname & ${Tools.round(baseGB, 2)} G & ${Tools.round(overhead,4)} \\\\")
    }
    fileout.println(
      """\hline
        |\end{tabular}
        |""".stripMargin)

  }


  def storage(dc: DataCube, name: String) = {
    val fileout = new PrintStream(s"expdata/Storage_${name}.csv")
    fileout.println("#Dimensions,#Cuboids,Storage Consumption,Avg Cuboid Size")
    val cubs = dc.cuboids.groupBy(_.n_bits).mapValues { cs =>
      val n = cs.length
      val sum = cs.map(_.numBytes).sum
      val avg = (sum / n)
      s"$n,$sum,$avg"
    }
    cubs.toList.sortBy(_._1).foreach { case (k, v) => fileout.println(s"$k,$v") }
  }

  def queryDistribution(cg: CubeGenerator) = {
    val sch = cg.schema()
    val qs = sch.root.queriesUpto(25).groupBy(_.length).mapValues(_.size).toSeq.sortBy(_._1)
    val fileout = new PrintStream(s"expdata/QueryDistribution_${cg.inputname}.csv")
    qs.foreach { case (k, v) => fileout.println(s"$k, $v")
    }
  }

  def genQueries(sch: StructuredDynamicSchema, nq: Int) = {
    //SBJ: TODO  Bug in Encoder means that all bits are shifed by one. The queries with max bit cannot be evaluated
    val allQueries = sch.root.queriesUpto(25).filter(!_.contains(sch.n_bits)).groupBy(_.length).mapValues(_.toVector)

    Vector(5, 10, 15, 20, 25).flatMap { i =>
      //val i = 4 * j
      val n = allQueries(i).size
      val qr = if (n <= nq + 10) allQueries(i) else {
        val idx = Util.collect_n(nq, () => scala.util.Random.nextInt(n))
        val r2 = idx.map(x => allQueries(i)(x))
        r2
      }
      qr
    }
  }

  def lpp_full_qsize(isSMS: Boolean)(implicit shouldRecord: Boolean) = {
    val cg = SSB(100)
    val param = "15_25_3"
    val name = (if (isSMS) "_sms_" else "_rms2_") + param
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load2(fullname, cg.inputname + "_base")
    val sch = cg.schema()
    val qss = List(6, 9, 12)
    val nq = 30
    val queries = qss.flatMap { qs =>
      (0 until nq).map(_ => sch.root.samplePrefix(qs)).distinct
    }

    //val numQs = sch.root.numPrefixUpto(15)
    //(0 until 15).map(i => println(s"$i => " + numQs(i)))
    import RationalTools._
    val expt = new LPSolverFullExpt[Rational](dc, fullname)
    if (shouldRecord) expt.warmup(5)
    queries.foreach(q => expt.run(q, true))
  }

  def lpp_online_qsize(isSMS: Boolean)(implicit shouldRecord: Boolean) = {
    val cg = SSB(100)
    val param = "15_25_3"
    val name = (if (isSMS) "_sms_" else "_rms2_") + param
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load2(fullname, cg.inputname + "_base")
    val sch = cg.schema()
    val qss = List(6, 8, 10)
    val nq = 30
    val queries = qss.flatMap { qs =>
      (0 until nq).map(_ => sch.root.samplePrefix(qs)).distinct
    }

    //val numQs = sch.root.numPrefixUpto(15)
    //(0 until 15).map(i => println(s"$i => " + numQs(i)))
    import RationalTools._
    val expt = new LPSolverOnlineExpt[Rational](dc, fullname)
    if (shouldRecord) expt.warmup(5)
    queries.foreach(q => expt.run(q, true))
  }

  def uniform_full_qsize(isSMS: Boolean)(implicit shouldRecord: Boolean) = {
    val cg = SSB(100)
    val param = "15_25_3"
    val name = (if (isSMS) "_sms_" else "_rms2_") + param
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load2(fullname, cg.inputname + "_base")
    val sch = cg.schema()
    val qss = List(6, 9, 12, 15)
    val nq = 30
    val queries = qss.flatMap { qs =>
      (0 until nq).map(_ => sch.root.samplePrefix(qs)).distinct
    }

    //val numQs = sch.root.numPrefixUpto(15)
    //(0 until 15).map(i => println(s"$i => " + numQs(i)))

    val expt = new UniformSolverFullExpt[Double](dc, fullname)
    if (shouldRecord) expt.warmup(5)
    queries.foreach(q => expt.run(q, true))
  }

  def uniform_online_qsize(isSMS: Boolean)(implicit shouldRecord: Boolean) = {
    val cg = SSB(100)
    val param = "15_25_3"
    val name = (if (isSMS) "_sms_" else "_rms2_") + param
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load2(fullname, cg.inputname + "_base")
    val sch = cg.schema()
    val qss = List(6, 9, 12, 15)
    val nq = 30
    val queries = qss.flatMap { qs =>
      (0 until nq).map(_ => sch.root.samplePrefix(qs)).distinct
    }

    val expt = new UniformSolverOnlineExpt[Double](dc, fullname)
    if (shouldRecord) expt.warmup(5)
    queries.foreach(q => expt.run(q, true))
  }

  def uniform_qsize(isSMS: Boolean)(implicit shouldRecord: Boolean): Unit = {
    val cg = SSB(100)
    val param = "15_28_0"
    val name = (if (isSMS) "_sms_" else "_rms2_") + param
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load2(fullname, cg.inputname + "_base")
    val sch = cg.schema()
    val qss = List(6, 9, 12, 15, 18)
    val nq = 100
    val queries = qss.flatMap { qs =>
      (0 until nq).map(_ => sch.root.samplePrefix(qs)).distinct
    }
    val exptfull = new UniformSolverFullExpt[Double](dc, fullname)
    if (shouldRecord) exptfull.warmup()
    val ql = queries.length
    queries.zipWithIndex.foreach{case(q,i) =>
      println(s"Full Query $i/$ql")
      exptfull.run(q)
    }

    val exptonline = new UniformSolverOnlineExpt[Double](dc, fullname)
    if (shouldRecord) exptonline.warmup()
    queries.zipWithIndex.foreach{ case (q, i) =>
      println(s"Online Query $i/$ql")
      exptonline.run(q)
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

   params.foreach{ p =>
      val fullname = cg.inputname + (if (isSMS) "-sms_" else "-rms2_") + s"${p._1}_${p._2}_0"
      val dc =  PartialDataCube.load2(fullname, cg.inputname + "_base")

     val exptfull = new UniformSolverFullExpt[Double](dc, fullname)
     if(shouldRecord) exptfull.warmup()
     val ql = queries.length
     queries.zipWithIndex.foreach{case(q,i) =>
       println(s"Full Query $i/$ql")
       exptfull.run(q)
     }

     val exptonline = new UniformSolverOnlineExpt[Double](dc, fullname)
     if(shouldRecord) exptonline.warmup()
     queries.zipWithIndex.foreach{ case (q, i) =>
     println(s"Online Query $i/$ql")
       exptonline.run(q)
     }

     dc.cuboids.head.backend.reset
    }

  }
  def mbonline()(implicit shouldRecord: Boolean) = {
    List(Uniform, Normal, LogNormal, Exponential).foreach { sample =>
      val cg = MicroBench(15, sample)
      val fullname = cg.inputname + "_all"
      val dc = DataCube.load2(fullname)
      val qs = List(6, 9, 12, 15).map(0 until _)
      val expt = new UniformSolverOnlineExpt[Double](dc, fullname, true)
      if (shouldRecord) {
       //Cannot use default warmup because of "containsAllCuboid" set to true
        (1 to 6).foreach(i => expt.run(0 until i, false))
      }
      qs.foreach(q => expt.run(q, true))
    }
  }
def debug(): Unit = {
  implicit val shouldRecord = false
  val cg = SSB(100)
  val isSMS = true
  val param = "15_28_0"
  val name = (if (isSMS) "_sms_" else "_rms2_") + param
  val fullname = cg.inputname + name
  val dc = PartialDataCube.load2(fullname, cg.inputname + "_base")
  val sch = cg.schema()
  val q1 = List(141,142,143,144,165,192)
  val q2 = List(116,117,118,119,120,129,130,131,137,138,139,155,172,180,192)
  val queries = List(q1, q2)
  ////val numQs = sch.root.numPrefixUpto(15)
  ////(0 until 15).map(i => println(s"$i => " + numQs(i)))
  //
  val expt = new UniformSolverOnlineExpt[Double](dc, fullname)
  //import RationalTools._
  //val expt = new LPSolverFullExpt[Rational](dc, fullname)
  expt.warmup(10)
  queries.foreach(q => expt.run(q, true))
  //val sample = Exponential
  //val cg = MicroBench(15, sample)

  //val fullname = cg.inputname + "_all"
  //val dc = DataCube.load2(fullname)

  //val (sch, r_its) = cg.generate2()
  //sch.initBeforeEncode()
  //val dc = new DataCube(MaterializationScheme.all_cuboids(cg.n_bits))
  //dc.build(CBackend.b.mkParallel(sch.n_bits, r_its))

  //val q = 0 until 15
  //val res = dc.naive_eval(q)
  //val resMax = res.max.toInt
  //val step = math.max(resMax/1000,1)
  //res.groupBy(x => (x.toInt/step) * step).mapValues(_.length).toList.sortBy(_._1).foreach(println)

  //val expt = new UniformSolverOnlineExpt[Double](dc, fullname, true)
  //expt.run(q)
}
  def main(args: Array[String]) {
    implicit val shouldRecord = true
    debug()
    //lpp_full_qsize(true)
    //lpp_full_qsize(false)
    //lpp_online_qsize(true)
    //lpp_online_qsize(false)
    //uniform_online_qsize(true)
    //uniform_online_qsize(false)
    //uniform_full_qsize(true)
    //uniform_full_qsize(false)
    //uniform_cubes(true, 10)
    //uniform_cubes(false, 10)
    //uniform_cubes(true, 12)
    //uniform_cubes(false, 12)
    //uniform_qsize(true)
    //uniform_qsize(false)
    //mbonline()
    //cubestats1()
    //cubestats2()
    //multi_storage(true)
    //multi_storage(false)
  }

  def oldmain(args: Array[String]): Unit = {

    //val cg = Iowa
    //val lrf = -16
    //val lrfs = List(-17.0, -16.0, -15.0)
    //val lbase = 0.19

    //implicit val shouldRecord = false
    //val lrf = -29
    //val lbase = 0.184
    //val cg = SSB(10)


    //val cg = SSB(1)
    //val lrf = -27
    //val lbase = 0.195

    //val cg = SSB(100)
    //val lrf = -32
    //val lbase = 0.19

    val cg = NYC
    //val lrf = -80.6
    //val lbase = 0.19


    //val cg = MicroBench(20, Normal)

    val p1 = List((16, 21, 0), (16, 25, 0), (16, 25, 4), (14, 19, 0))
    val p2 = List((15, 20, 0), (15, 24, 0), (15, 25, 3), (14, 23, 0), (14, 25, 2))
    //val p = List((15, 25, 3))
    val params = p1
    val sms_names = params.map(p => "sms_" + p._1 + "_" + p._2 + "_" + p._3)
    //val rms_names = params.map(p => "rms2_" + p._1 + "_" + p._2 + "_" + p._3)
    val names = sms_names // ++ rms_names

    //val (sch, dc) = cg.loadBase()
    //val (sch, dc) = cg.load(lrf, lbase)
    //val (sch, dc) = cg.load2()
    //val (sch, dcs) = cg.multiload(lrfs.map(lrf => (lrf, lbase)))
    val sch = cg.schema()
    //val dcs = names.map { n => n -> PartialDataCube.load2(cg.inputname + "_" + n, cg.inputname + "_base") }
    //val dc = cg.loadDC(lrf, lbase)
    //val cubename =  "sms_15_25_3"
    //val cubename =  "sms_13_20"
    //val cubename =  "base"
    //val (sch, dc) = cg.loadPartial(cubename)
    //val dc = cg.dc


    //val dc2 = new DataCube(SchemaBasedMaterializationScheme(sch))
    //dc2.buildFrom(dc)
    //dc2.save2(s"${cg.inputname}_sch")


    val qs = List(17).flatMap(q => (0 until 3).map(i => sch.root.samplePrefix(q)))
    //val qs = List(14, 18).flatMap(q => (0 until 30).map(i => sch.root.samplePrefix(q)))
    //val qs = List(10).flatMap(q => (0 until 100).map(i => sch.root.samplePrefix(q))).distinct
    //val qs = List(List(143, 144, 165, 170, 171, 172, 191, 192))
    //val qs = List((0 until 12).toList)
    //val qs = List(List(13, 12, 11, 10, 52, 51, 50, 49, 48, 47, 46, 82, 81, 80, 79))
    //val qs = (0 to 30).map{i => Tools.rand_q(dc.m.n_bits, 10)}

    //dcs.foreach { case (cubename, dc) =>
    //val cubename = s"${lrf}-${lbase}"
    //val cubename = s"sch"
    //val cubename = "all"
    //full(s"${cg.inputname}-${cubename}", dc, qs)
    //online(s"${cg.inputname}-${cubename}", dc, qs)
    //mbonline(s"${cg.inputname}-${cubename}", dc, qs)
    //  storage(dc, cg.inputname+"_"+cubename)
    //Profiler.print()
    //}
    //queryDistribution(cg)

  }
}
