package examples

import backend.CBackend
import frontend.generators._
import frontend.gui.FeatureFrame
import util.{BigBinary, Profiler}

object DemoTxt {

  import frontend._, backend._, core._, core.RationalTools._

  def test() = {
    val data  = (0 to 15).map(i => BigBinary(i) -> i)
    val nbits = 10
    val dc = new DataCube(RandomizedMaterializationScheme(nbits, 1, 1))
    dc.build(CBackend.b.mk(nbits, data.toIterator))
    dc.save("test")
  }
  def loadtest(): Unit = {
    core.DataCube.load("test")
  }

  def iowa(): Unit = {
    val sch = new schema.DynamicSchema
    //val name = "Iowa200k"
    //val R = Profiler("Sch.Read"){sch.read(s"/Users/sachin/Downloads/$name.csv")}
    val name = "Iowa2M"
    val R = Profiler("Sch.Read"){sch.read(s"$name.csv")}
    println("NBITS =" + sch.n_bits)
    Profiler.print()

    val dc  = new DataCube(MaterializationScheme.only_base_cuboid(sch.n_bits))
    Profiler("Build"){dc.build(CBackend.b.mk(sch.n_bits, R.toIterator))}
    Profiler.print()
    dc.save(name+"_base")
  }
  def investment(): Unit = {

    val sch = new schema.DynamicSchema
    val R = sch.read("investments.json", Some("k_amount"))
    val dc = new DataCube(RandomizedMaterializationScheme(sch.n_bits, .0000000008, 10))
    dc.build(CBackend.b.mk(sch.n_bits, R.toIterator))

    /*
    Exploration.col_names(sch)

    val q1 = sch.columns("company").bits.toList
    val result = dc.naive_eval(q1)
    sch.decode_dim(q1).zip(result)

    Exploration.dist(sch, dc, "company")

    val grp_bits = 2
    val q2 = sch.columns("date").bits.toList.drop(grp_bits)
    Exploration.nat_decode_dim(sch, "date", grp_bits).zip(dc.naive_eval(q2)).filter(
      x => (x._1(0) >= 1996) && (x._1(0) < 2020))
  */

    val q = List(0, 12, 1)

    // solves to df=2 using only 2-dim cuboids
    //val s = dc.solver[Rational](q, 2)
    //s.compute_bounds

    // runs up to the full cube
    dc.naive_eval(q)

    // this one need to run up to the full cube
    val od = OnlineDisplay(sch, dc, PrettyPrinter.formatPivotTable)
    od.l_run(q, 2)
  }

  def shoppen() = {
    // exploration example -- unknown file

    val sch = new schema.DynamicSchema
    val R = sch.read("Shoppen.json")
    val dc = new DataCube(RandomizedMaterializationScheme(sch.n_bits, .005, 1.02))
    dc.build(CBackend.b.mk(sch.n_bits, R.toIterator))

    Exploration.col_names(sch)

  }


  def large() = {
    // "Large" example:

    /*
        import frontend._
        val dcw = experiments.Tools.mkDC(100, 0.1, 1.05, 100000, Sampling.f2)
        dcw.save("s2_d100_100k.dc")
    */

    val sch = schema.StaticSchema.mk(100)
    val dc = core.DataCube.load("s2_d100_100k.dc")

    //dc.m.projections
    //dc.m.projections.map(_.length).groupBy(x => x).mapValues(_.length).toList.sorted

    //dc.naive_eval(List(0, 1, 2, 3))

    val od = OnlineDisplay(sch, dc, PrettyPrinter.formatPivotTable)

    //od.l_run(List(0, 1, 2, 3), 4)
    od.l_run(List(0, 1, 2, 3, 4, 5), 50)

    //od.ui.visible = false

  }

  def feature() = {

    val n_cols = 3
    val n_bits_per_col = 8
    val n_bits = n_cols * n_bits_per_col
    val n_rows = 1000

    //val columnMap = Map(0 -> "Time", "1" ->)
    val sch = schema.StaticSchema.mk(n_cols, n_bits_per_col, Map(0 -> "Time", 1 -> "Product", 2 -> "Location"))

    //--------------CUBE DATA GENERATION-------------
    val vgs = collection.mutable.ArrayBuffer[ValueGenerator]()
    //vgs += ConstantValueGenerator(50)
    vgs += RandomValueGenerator(2)
    //vgs += SinValueGenerator(List(0, 1, 2, 3), List(13, 17), 0, 1, 2)
    //vgs += SinValueGenerator(List(1, 3, 5), List(12, 25, 32), 0, 1, 4)
    //vgs += SinValueGenerator(List(1, 5, 7), List(11, 12), 0, 1, 8)
    //vgs += SinValueGenerator(List(1, 2, 5), List(20, 22), 0, 1, 8)

    //vgs += TrendValueGenerator(List(4, 5, 6, 7), List(13, 14, 15, 19, 20, 21, 22, 23), 147, 1, 15*256)
    //vgs += TrendValueGenerator(List(6, 7), List(22, 23), 3, -1, 20)
    //  //vgs += TrendValueGenerator( List(4, 5, 6, 7), List(), 0, 1, 10)
    // //vgs += TrendValueGenerator( List(6, 7), List(13,14,15),5, 1, 1)
    //vgs += TrendValueGenerator(List(2, 3, 4, 5, 6, 7), List(15), 0, -1, 30)
    val vg = SumValueGenerator(vgs)

    val R = TupleGenerator2(sch, n_rows, Sampling.f1, vg)
    println("mkDC: Creating maximum-granularity cuboid...")
    val fc = CBackend.b.mk(n_bits, R)
    println("...done")
    val m = RandomizedMaterializationScheme(n_bits, 0.4, 1.1)
    val dcw = new DataCube(m);
    dcw.build(fc)
    dcw.save("trend.dc")
    // ----------------- END CUBE DATA GENERATION ----------

    val dc = core.DataCube.load("trend.dc")
    println("Materialization Schema" + dc.m)

    val display = FeatureFrame(sch, dc, 50)

  }

  def parPlan() = {
    val m = RandomizedMaterializationScheme(100, 0.1, 1.05)
    println(m.projections.size)
    val seq = m.create_build_plan()
    val par = m.create_parallel_build_plan(8)

    println(seq.map(kv => kv._3 -> kv._2).mkString("", " ", "\n"))

    par.foreach(pseq => println(pseq.map(kv => kv._3 -> kv._2).mkString("", " ", "\n")))
  }

  def sample(n: Int): Unit = {
    val map = collection.mutable.HashMap[Int, Int]() withDefaultValue(0)
    (1 to n).foreach{  i =>
      val s = Sampling.f2(1 << 20)
      map(s) += 1
    }
    //map.foreach(println)
    println(map.size)
    map.filter(_._2 > 4).foreach(println)
  }
  def main(args: Array[String]): Unit = {
    //iowa()
    test()
    //loadtest()
    //investment()
  //sample(1000)
    //large()
    //feature()
    //parPlan()
  }

}