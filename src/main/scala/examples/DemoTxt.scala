package examples

import frontend.gui.FeatureFrame

object DemoTxt {

  import frontend._, backend._, core._, core.RationalTools._

  def investment(): Unit = {

    val sch = new schema.DynamicSchema
    val R = sch.read("investments.json", Some("k_amount"))
    val dc = new DataCube(RandomizedMaterializationScheme(sch.n_bits, .5, 1.2))
    dc.build(CBackend.b.mk(sch.n_bits, R.toIterator))

    Exploration.col_names(sch)

    val q1 = sch.columns("company").bits.toList
    val result = dc.naive_eval(q1)
    sch.decode_dim(q1).zip(result)

    Exploration.dist(sch, dc, "company")

    val grp_bits = 2
    val q2 = sch.columns("date").bits.toList.drop(grp_bits)
    Exploration.nat_decode_dim(sch, "date", grp_bits).zip(dc.naive_eval(q2)).filter(
      x => (x._1(0) >= 1996) && (x._1(0) < 2020))

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

    val ncols = 100
    val xcols = List(5, 6, 7, 8)
    val zcols = List(9, 10, 11)
    val zval = 5
    val nrows = 10000
    val sch = schema.StaticSchema.mk(ncols)

    //val dcw = frontend.experiments.Tools.mkDC2(ncols, 0.1, 1.05, nrows, xcols, zcols, zval, Sampling.f1)
    //dcw.save("trend.dc")

    val dc = core.DataCube.load("trend.dc")

    def draw(sch: schema.Schema, bou: Seq[(Interval[Rational], Int)]) = bou.map{case (r, id) => id.toBinaryString + " :: " + r.format(_.toString)}mkString("\n\n", "\n", "\n\n")

    val display = FeatureDisplay(sch, dc, draw)
    display.run(xcols, zcols, zval)(50)
  }

  def main(args: Array[String]): Unit = {
    //large()
    feature()
    //val f = FeatureFrame()
  }

}