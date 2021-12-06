package experiments

import core.{DataCube, Rational, SchemaBasedMaterializationScheme}
import frontend.experiments.Tools
import frontend.generators.{CubeGenerator, Iowa, SSB}
import frontend.schema.StructuredDynamicSchema
import util.Util

import java.io.PrintStream
import java.time.Instant
import scala.util.Random

object Experimenter {

  def randomQueries(nb: Int) = {
    (0 to 30).map { i =>
      val s = Random.nextInt(4) + 4
      Tools.rand_q(nb, s)
    }
  }

  def full(name: String, dc: DataCube, qs: Seq[Seq[Int]])(implicit record: Boolean) = {
    import core.RationalTools._
    val expt = new UniformSolverExpt[Double](dc, name)
    qs.foreach(q => expt.compare(q, true))
  }
  def online(name: String, dc: DataCube, qs: Seq[Seq[Int]])(implicit record: Boolean) = {
    val expt = new UniformSolverOnlineExpt[Double](dc, name)
    //Warm up
    randomQueries(dc.m.n_bits).foreach(q => expt.compare(q, false))
    println("Warmup Complete")
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
    val timestamp = Instant.now().toString
    val fileout = new PrintStream(s"expdata/Storage_${name}_${timestamp}.csv")
    fileout.println("#Dimensions,#Cuboids,Storage Consumption,Avg Cuboid Size")
    val cubs = dc.cuboids.groupBy(_.n_bits).mapValues{cs =>
      val n = cs.length
      val sum = cs.map(_.numBytes).sum
      val avg = (sum/n).toInt
      s"$n,$sum,$avg"
    }
    cubs.toList.sortBy(_._1).foreach{case (k, v) => fileout.println(s"$k,$v")}
  }

  def genQueries(sch: StructuredDynamicSchema) = {
    //SBJ: TODO  Bug in Encoder means that all bits are shifed by one. The queries with max bit cannot be evaluated
    val allQueries = sch.queries.filter(!_.contains(sch.n_bits)).groupBy(_.length).mapValues(_.toVector)

   Vector(8, 10, 12).flatMap { i =>
      //val i = 4 * j
      val n = allQueries(i).size
      val qr = if (n <= 40) allQueries(i) else {
        val idx = Util.collect_n(30, () => scala.util.Random.nextInt(n))
        val r2 = idx.map(x => allQueries(i)(x))
        r2
      }
      qr
    }
  }

  def main(args: Array[String]): Unit = {

    val cg = Iowa
    val lrf = -16
    //val lrfs = List(-17.0, -16.0, -15.0)
    val lbase = 0.19

  implicit val shouldRecord = true
    //val lrf = -29
    //val lbase = 0.184
    //val cg = SSB(10)


    //val cg = SSB(1)
    //val lrf = -27
    //val lbase = 0.195

    val (sch, dc) = cg.load(lrf, lbase)
    //val (sch, dc) = cg.load2()
    //val (sch, dcs) = cg.multiload(lrfs.map(lrf => (lrf, lbase)))

    //val dc2 = new DataCube(SchemaBasedMaterializationScheme(sch))
    //dc2.buildFrom(dc)
    //dc2.save2(s"${cg.inputname}_sch")

    val qs = genQueries(sch)
    //val qs = List(List(13, 12, 11, 10, 52, 51, 50, 49, 48, 47, 46, 82, 81, 80, 79))


    //dcs.foreach{case ((lrf, lbase), dc) =>
    val cubename = s"${lrf}-${lbase}"
    //val cubename = s"sch"
      online(s"${cg.inputname}-${cubename}", dc, qs)
    //}
    //storage(dc, cg.inputname)

  }
}
