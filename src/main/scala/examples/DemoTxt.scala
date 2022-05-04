package examples

import combinatorics.Combinatorics.comb
import core.SolverTools._
import core.solver.MomentSolverAll
import core.solver.Strategy._
import frontend.TUPLES_PREFIX
import frontend.experiments.Tools
import frontend.generators._
import frontend.gui.{FeatureFrame, FeatureFrameSSB}
import frontend.schema.encoders.StaticNatCol
import frontend.schema.{LD2, StaticSchema2}
import util._

import scala.util.Random

object DemoTxt {

  import backend._
  import core.RationalTools._
  import core._
  import frontend._

  def momentSolver(): Unit = {
    val solver = new MomentSolverAll[Rational](3, Avg2)
    val actual = Array(1, 3, 2, 1, 5, 1, 0, 2).map(_.toDouble)
    solver.add(List(0, 1), Array(6, 4, 2, 3).map(Rational(_, 1)))
    solver.add(List(1, 2), Array(4, 3, 6, 2).map(Rational(_, 1)))
    solver.add(List(0, 2), Array(3, 4, 5, 3).map(Rational(_, 1)))
    //solver.add(List(0,1, 2), Array(1, 3, 2, 1, 5, 1, 0, 2).map(Rational(_, 1)))
    println(solver.sumValues.mkString(" "))
    solver.fillMissing()
    val result = solver.fastSolve().map(_.toDouble)
    println(result.mkString(" "))
    println("Error = " + error(actual, result.toArray))
    solver.verifySolution()


    //val solver = new UniformSolver[Rational](4)
    //solver.sumValues.indices.foreach(i => solver.sumValues(i) = 1)
    //List(220721,84949,94282,36864,110130,42358,47570,18534,22,0,12,0,10,0,6).map(i => Rational(i, 1)).zipWithIndex.foreach{
    //  case (v, i) =>
    //    solver.sumValues(i) = v
    //    solver.knownSums += i
    //}

    //val res = solver.fastSolve()
    //println(res.mkString(" "))
  }

  def momentSolver2() = {
    val solver = new MomentSolverAll[Rational](3, CoMoment4)
    val actual = Array(0, 1, 3, 1, 7, 2, 3, 0).map(_.toDouble)
    solver.add(List(2), Array(5, 12).map(Rational(_, 1)))
    solver.add(List(0, 1), Array(7, 3, 6, 1).map(Rational(_, 1)))
    solver.add(List(1, 2), Array(1, 4, 9, 3))
    solver.add(List(0, 2), Array(3, 2, 10, 2).map(Rational(_, 1)))
    println("Moments before =" + solver.sumValues.mkString(" "))
    solver.fillMissing()
    println("Moments after =" + solver.sumValues.mkString(" "))
    val result = solver.fastSolve().map(_.toDouble)
    println(result.mkString(" "))
    println("Error = " + error(actual, result.toArray))
    solver.verifySolution()
  }

  def test() = {
    val data = (0 to 15).map(i => BigBinary(i) -> i.toLong)
    val nbits = 10
    val dc = new DataCube(RandomizedMaterializationScheme(nbits, 1, 1))
    dc.build(CBackend.b.mk(nbits, data.toIterator))
    dc.save("test")
  }

  def loadtest(): Unit = {
    val dc = core.DataCube.load("test")
    println(dc.naive_eval(List(3, 4, 5, 6)).mkString("  "))
  }

  def solve(dc: DataCube)(qsize: Int) = {
    Profiler.resetAll()

    val q = Tools.rand_q(dc.m.n_bits, qsize)
    println("Query =" + q)

    val solver = Profiler("Solver") {
      val s = Profiler("SolverInit") {
        dc.solver[Rational](q, qsize - 1)
      }
      Profiler("SolverCB") {
        s.compute_bounds
      }
      s
    }
    val res1 = solver.bounds.map(i => i.lb.get + ":" + i.ub.get).mkString(" ")
    val res2 = Profiler("Naive") {
      dc.naive_eval(q)
    }.mkString(" ")
    println(res1)
    println(res2)
    println("DF = " + solver.df)
    Profiler.print()
  }

  def printMatStats(ms: MaterializationScheme) = {
    val m = ms.asInstanceOf[RandomizedMaterializationScheme]
    (0 until m.n_bits).foreach { d =>
      val total = comb(m.n_bits, d)
      val nproj = m.n_proj_d(d)
      println(s" $d :: $nproj/$total ")
    }
  }

  def solve2(dc: DataCube)(q: List[Int]) = {
    Profiler.resetAll()

    println("Query =" + q)

    val solver = Profiler("Solver") {
      val s = Profiler("SolverInit") {
        dc.solver[Rational](q, q.size - 1)
      }
      Profiler("SolverCB") {
        s.compute_bounds
      }
      s
    }
    val res1 = solver.bounds.map(i => i.lb.get + ":" + i.ub.get).mkString(" ")
    val res2 = Profiler("Naive") {
      dc.naive_eval(q)
    }.mkString(" ")
    println(res1)
    println(res2)
    println("DF = " + solver.df)
    Profiler.print()
  }


  def investment(): Unit = {

    val sch = new schema.DynamicSchema

    val R = sch.read("investments.json", Some("k_amount"), _.asInstanceOf[Int].toLong)
    //R.map{case (k, v) => sch.decode_tuple(k).mkString("{",",","}") + "  " + k + " " + v }.foreach(println)

    val basecuboid = CBackend.b.mk(sch.n_bits, R.toIterator)

    val matscheme = RandomizedMaterializationScheme2(sch.n_bits, 8, 4, 4)
    val dc = new DataCube(matscheme)
    dc.build(basecuboid)

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

    val qV = List(0, 12) //Company
    val qH = List(1) //even or odd years

    //FIXME: Replace query as Set[Int] instead of Seq[Int]. Until then, we assume query is sorted in increasing order of bits
    val q = (qV ++ qH).sorted

    // solves to df=2 using only 2-dim cuboids
    //val s = dc.solver[Rational](q, 2)
    //s.compute_bounds

    // runs up to the full cube
    val r = dc.naive_eval(q)
    println("r =" + r.mkString(" "))

    // this one need to run up to the full cube
    val od = OnlineDisplay(sch, dc, PrettyPrinter.formatPivotTable(sch, qV, qH)) //FIXME: Fixed qV and qH. Make variable depending on query
    od.l_run(q, 2)
  }

  def cooking(): Unit = {


    val userCube = UserCube.createFromJson("recipes.json", "rating")

    //var matrix = userCube.queryMatrix(List(("spicy", 1), ("Region", 2)), List(("Vegetarian", 1)), AND, MOMENT)
    /*var matrix = userCube.querySliceMatrix(List(("Region", 3, List("India")), ("spicy", 1, List("<=1")), ("Type", 1, Nil)),List(), AND, MOMENT)
    println(matrix.toString(Int.MaxValue, Int.MaxValue) + "\n")*/
    //val qH = userCube.query(List())

    //matrix = userCube.queryMatrix(List(("spicy", 1), ("Region", 2)), List(("Vegetarian", 1)), OR, MOMENT)
    /*matrix = userCube.querySliceMatrix(List(("Region", 3, List("India")), ("spicy", 1, List(">=0")), ("Type", 1, Nil)),List(), OR, MOMENT)
    println(matrix.toString(Int.MaxValue, Int.MaxValue))*/

    println(userCube.query(List(("Region", 3, List("India")), ("spicy", 1, List(">=0")), ("Type", 1, Nil)), List(), AND, MOMENT, TUPLES_PREFIX).asInstanceOf[Array[String]].mkString("Array(", ",\n", ")"))

    //val array = userCube.queryArray(List(("Region", 2), ("Type", 1)), List(("Vegetarian", 1)), "moment")
    //val array = userCube.queryArrayS(List(("Region", 3, List("India")), ("spicy", 1, List()), ("Type", 1, List())),List(("Vegetarian", 1, List())), AND, MOMENT)
    //println(array._3.mkString("Array(", ", ", ")"))

  
  }

  def shoppen() = {
    // exploration example -- unknown file

    val sch = new schema.DynamicSchema
    val R = sch.read("Shoppen.json")
    val dc = new DataCube(RandomizedMaterializationScheme(sch.n_bits, .005, 1.02))
    dc.build(CBackend.b.mk(sch.n_bits, R.toIterator))
    Exploration.col_names(sch)
  }

  def feature() = {

    val n_cols = 3
    val n_bits_per_col = 20
    val n_bits = n_cols * n_bits_per_col
    val n_rows = 100 * (1 << n_bits_per_col) + 10 //60 * 1000 * 1000

    val time = new LD2("Time", new StaticNatCol(0, (1 << n_bits_per_col) - 2, StaticNatCol.defaultToInt))
    val prod = new LD2("Product", new StaticNatCol(0, (1 << n_bits_per_col) - 2, StaticNatCol.defaultToInt))
    val loc = new LD2("Location", new StaticNatCol(0, (1 << n_bits_per_col) - 2, StaticNatCol.defaultToInt))
    val sch2 = new StaticSchema2(Vector(time, prod, loc))
    //val columnMap = Map(0 -> "Time", "1" ->)
    //val sch = schema.StaticSchema.mk(n_cols, n_bits_per_col, Map(0 -> "Time", 1 -> "Product", 2 -> "Location"))

    def prefix(c: Int, n: Int) = {
      val res = offset(c, 0 until n)
      println(s"prefix($c $n) = $res")
      res
    }

    def offset(c: Int, vs: Seq[Int]) = vs.reverse.map(n_bits_per_col * (c + 1) - 1 - _).toList

    //--------------CUBE DATA GENERATION-------------
    val vgs = collection.mutable.ArrayBuffer[ValueGenerator]()
    vgs += ConstantValueGenerator(100)
    vgs += RandomValueGenerator(10)
    //vgs += SinValueGenerator(List(0, 1, 2, 3), List(5).map(x => n_bits_per_col + x) ++ List(1).map(x => 2*n_bits_per_col + x), 0, 1, 2)
    vgs += SinValueGenerator(offset(0, List(2, 4, 6)), prefix(1, 1) ++ prefix(2, 1), 0, 1, 50)
    //vgs += SinValueGenerator(List(1, 5, 7), List(11, 12), 0, 1, 8)
    //vgs += SinValueGenerator(List(1, 2, 5), List(20, 22), 0, 1, 8)

    vgs += TrendValueGenerator(prefix(0, 4), prefix(1, 3) ++ prefix(2, 5), 147, 1, 15 * 256 * 1)
    vgs += TrendValueGenerator(prefix(0, 2), prefix(1, 2), 3, -1, 100)
    //  //vgs += TrendValueGenerator( List(4, 5, 6, 7), List(), 0, 1, 10)
    // vgs += TrendValueGenerator( prefix(0, 2), prefix(1, 3), 5, 1, 1*3)
    //vgs += TrendValueGenerator(List(2, 3, 4, 5, 6, 7), List(15), 0, -1, 30)
    val vg = SumValueGenerator(vgs)

    val R = ParallelTupleGenerator2(sch2, n_rows, 1000, Sampling.f1, vg).data
    println("mkDC: Creating maximum-granularity cuboid...")
    val fc = CBackend.b.mkParallel(n_bits, R)
    println("...done")
    //val m = RandomizedMaterializationScheme2(n_bits, 13,math.min(20, (math.log(n_rows)/math.log(2)-1).toInt))
    val m = MaterializationScheme.only_base_cuboid(n_bits)
    val dcw = new DataCube(m);
    dcw.build(fc)
    dcw.save2("trend")
    // ----------------- END CUBE DATA GENERATION ----------

    val dc = core.DataCube.load2("trend")
    println("Materialization Schema" + dc.m)

    val display = FeatureFrame(sch2, dc, 50)

  }

  def backend_naive() = {
    val n_bits = 10
    val n_rows = 40
    val n_queries = 10
    val query_size = 5
    val rnd = new Random(1L)
    val data = (0 until n_rows).map(i => BigBinary(rnd.nextInt(1 << n_bits)) -> rnd.nextInt(10).toLong)
    val fullcub = CBackend.b.mk(n_bits, data.toIterator)
    println("Full Cuboid data = " + data.mkString("  "))
    val dc = new DataCube(RandomizedMaterializationScheme(n_bits, 0.1, 1.8))
    dc.build(fullcub)
    (0 until n_queries).map { i =>
      val q = Tools.rand_q(n_bits, query_size)
      println("\nQuery =" + q)
      val res = dc.naive_eval(q)
      println("Result = " + res.mkString(" "))
    }
  }

  def ssb_demo() = {
    val sf = 100
    val cg = SSB(sf)
    //val sch = cg.schema()
    //val cols = sch.columnVector
    //
    //val odate = cols(0).encoder.asInstanceOf[StaticDateCol]
    //val ccity = cols(11)
    //val cnation = cols(12)
    //val cregion = cols(14)
    //
    //val mfgr = cols(18)
    //val category = cols(19)
    //val size = cols(23)
    //val color = cols(21)

    //println("odateyear = "+odate.yearCol.bits.mkString(","))
    //println("odateyearqtr = "+( odate.quarterCol.bits ++ odate.yearCol.bits) .mkString(","))
    //println("odateyearmonth =" + ( odate.monthCol.bits ++ odate.yearCol.bits).mkString(","))
    //println("odatedate =" + (odate.dayCol.bits ++ odate.monthCol.bits ++ odate.yearCol.bits).mkString(","))
    //println("ccity = "+ccity.encoder.bits.mkString(","))
    //println("cnat = "+cnation.encoder.bits.mkString(","))
    //println("cregion = "+cregion.encoder.bits.mkString(","))
    //println("mfgr =" + mfgr.encoder.bits.mkString(","))
    //println("category =" + category.encoder.bits.mkString(","))
    //println("size =" + size.encoder.bits.mkString(","))
    //println("color =" + color.encoder.bits.mkString(","))

    val dc = PartialDataCube.load2(cg.inputname + "_rms2_15_22_0", cg.inputname + "_base")
    val display = FeatureFrameSSB(sf, dc, 50)
  }

  def main(args: Array[String]): Unit = {
    //investment()
    //momentSolver()
    //momentSolver2()
    //backend_naive()
    //loadtest()
    //ssb_demo()
    cooking()
  }
}