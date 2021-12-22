package experiments

import core.{DataCube, PartialDataCube, Rational, SchemaBasedMaterializationScheme}
import frontend.experiments.Tools
import frontend.generators.{CubeGenerator, Iowa, NYC, SSB}
import frontend.schema.StructuredDynamicSchema
import util.{Profiler, Util}

import java.io.PrintStream
import java.time.Instant
import scala.util.Random

object Experimenter {

  def randomQueries(nb: Int) = {
    (0 to 100).map { i =>
      val s = Random.nextInt(4) + 4
      Tools.rand_q(nb, s)
    }
  }

  def fullwarmup() = {
    val dc = DataCube.load2("warmup")
    implicit val shouldRecord = false
    val expt = new UniformSolverExpt[Double](dc, "warmup")
    randomQueries(dc.m.n_bits).foreach(q => expt.compare(q, false))
    println("Warmup Complete")
  }

  def full(name: String, dc: DataCube, qs: Seq[Seq[Int]])(implicit record: Boolean) = {
    //fullwarmup()
    val expt = new UniformSolverExpt[Double](dc, name)
    qs.foreach(q => expt.compare(q, true))
  }

  def onlinewarmup() = {
    val dc = DataCube.load2("warmup")
    implicit val shouldRecord = false
    val expt = new UniformSolverOnlineExpt[Double](dc, "warmup")
    randomQueries(dc.m.n_bits).foreach(q => expt.compare(q, false))
    println("Warmup Complete")
  }

  def online(name: String, dc: DataCube, qs: Seq[Seq[Int]])(implicit record: Boolean) = {
    //onlinewarmup()
    val expt = new UniformSolverOnlineExpt[Double](dc, name)
    qs.foreach(q => expt.compare(q, true))
  }

  def multi_storage(cg: CubeGenerator, lrfs: Seq[Double], lbase: Double) = {
    val timestamp = Instant.now().toString
    val fileout = new PrintStream(s"expdata/MultiStorage_${cg.inputname}_${timestamp}.csv")

    lrfs.map { lrf =>
      val dc = cg.loadDC(lrf, lbase)
      dc.cuboids.groupBy(_.n_bits).mapValues(_.length).map { case (nb, nc) => s"$lrf \t $nb \t $nc" }
    }.foreach(fileout.println)
  }

  def storage(dc: DataCube, name: String) = {

    val fileout = new PrintStream(s"expdata/Storage_${name}.csv")
    fileout.println("#Dimensions,#Cuboids,Storage Consumption,Avg Cuboid Size")
    val cubs = dc.cuboids.groupBy(_.n_bits).mapValues{cs =>
      val n = cs.length
      val sum = cs.map(_.numBytes).sum
      val avg = (sum/n)
      s"$n,$sum,$avg"
    }
    cubs.toList.sortBy(_._1).foreach{case (k, v) => fileout.println(s"$k,$v")}
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

  def main(args: Array[String]): Unit = {

    //val cg = Iowa
    //val lrf = -16
    //val lrfs = List(-17.0, -16.0, -15.0)
    //val lbase = 0.19

  implicit val shouldRecord = true
    //val lrf = -29
    //val lbase = 0.184
    //val cg = SSB(10)


    //val cg = SSB(1)
    //val lrf = -27
    //val lbase = 0.195

    val cg = SSB(100)

    //val lrf = -32
    //val lbase = 0.19
    //val names1 = List((15, 19, 0), (15, 25, 3)).map { p => "sms_" + p._1 + "_" + p._2 + "_" + p._3 }
    //val names2 = List((15, 22, 0), (15, 25, 0)).map { p => "sms_" + p._1 + "_" + p._2 + "_" + p._3 }
    //val names = names1 ++ names2
    //val cg = NYC
    //val lrf = -80.6
    //val lbase = 0.19


      //val (sch, dc) = cg.loadBase()
    //val (sch, dc) = cg.load(lrf, lbase)
    //val (sch, dc) = cg.load2()
    //val (sch, dcs) = cg.multiload(lrfs.map(lrf => (lrf, lbase)))
    //val sch = cg.schema()
    //val dcs = names.map{n => n->PartialDataCube.load2(cg.inputname + "_"+n, cg.inputname + "_base")}
    //val dc = cg.loadDC(lrf, lbase)
    val cubename =  "sms_15_25_3"
    //val cubename =  "sms_13_20"
    //val cubename =  "base"
    val (sch, dc) = cg.loadPartial(cubename)


    //val dc2 = new DataCube(SchemaBasedMaterializationScheme(sch))
    //dc2.buildFrom(dc)
    //dc2.save2(s"${cg.inputname}_sch")

    if(shouldRecord) {
      //onlinewarmup()
      fullwarmup()
    }
    //val qs = List(20).flatMap(q => (0 until 1).map(i => sch.root.samplePrefix(q)))
    val qs = List(14, 18).flatMap(q => (0 until 30).map(i => sch.root.samplePrefix(q)))
    //val qs = List(10, 22).flatMap(q => (0 until 30).map(i => sch.root.samplePrefix(q)))
    //val qs = List(List(143, 144, 165, 170, 171, 172, 191, 192))

    //val qs = List(List(13, 12, 11, 10, 52, 51, 50, 49, 48, 47, 46, 82, 81, 80, 79))
    //val qs = (0 to 30).map{i => Tools.rand_q(dc.m.n_bits, 10)}

    //dcs.foreach{case (cubename, dc) =>
    //val cubename = s"${lrf}-${lbase}"
    //val cubename = s"sch"
    full(s"${cg.inputname}-${cubename}", dc, qs)
    //  online(s"${cg.inputname}-${cubename}", dc, qs)
    //  storage(dc, cg.inputname+"_"+cubename)
      //Profiler.print()
    //}
    //queryDistribution(cg)

  }
}
