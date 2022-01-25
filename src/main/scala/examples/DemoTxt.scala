package examples

import backend.CBackend
import breeze.io.{CSVReader, CSVWriter}
import combinatorics.Combinatorics.{comb, comb2, mk_comb_bi}
import core.solver.UniformSolver
import experiments.UniformSolverFullExpt
import frontend.experiments.Tools
import frontend.generators._
import frontend.gui.{FeatureFrame, FeatureFrameSSB}
import util._
import core.SolverTools._
import core.solver.Strategy.{CoMoment, CoMoment3, Cumulant}
import frontend.schema.encoders.{DateCol, MemCol, NatCol, NestedMemCol, PositionCol, StaticDateCol, StaticNatCol}
import frontend.schema.{BitPosRegistry, DynamicSchema, LD2, StaticSchema2, StructuredDynamicSchema}

import java.io.{FileReader, FileWriter}
import java.util.Date
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object DemoTxt {

  import frontend._, backend._, core._, core.RationalTools._

  def uniformSolver(): Unit = {
    val solver = new UniformSolver[Rational](3, CoMoment3)
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


  def iowa(): Unit = {
    val sch = new schema.DynamicSchema
    val name = "Iowa200k"
    val rf = Math.pow(2, -195)
    val base = 2
    val R = Profiler("Sch.Read") {
      sch.read(s"/Users/sachin/Downloads/$name.csv", Some("Sale (Dollars)"), o => (o.asInstanceOf[String].toDouble * 100).toLong)
    }
    //val name = "Iowa2M"
    //val R = Profiler("Sch.Read"){sch.read(s"$name.csv")}
    println("NBITS =" + sch.n_bits)
    sch.columnList.map(kv => kv._1 -> kv._2.bits.length).sortBy(_._2).foreach(println)
    Profiler.print()
    //
    //val dc  = new DataCube(RandomizedMaterializationScheme(sch.n_bits, rf, base))
    //Profiler("Build"){dc.build(CBackend.b.mk(sch.n_bits, R.toIterator))}
    //Profiler.print()
    //dc.save2(s"${name}_volL_2p-195_2")
  }

  def iowa2() = {
    val rf = Math.pow(2, -115)
    val base = 1.5
    val dcBase = DataCube.load("Iowa200k_base")
    val dc = new DataCube(RandomizedMaterializationScheme(dcBase.m.n_bits, rf, base))
    dc.buildFrom(dcBase)
    dc.save("Iowa200_all")
  }

  def test1() = {
    implicit val bpos = new BitPosRegistry
    val e2 = LD2("col1", new NatCol(1000))
    val e1 = LD2("col1", new NatCol(60))
    val s = new StructuredDynamicSchema(Vector(e2, e1))
    println("Schema bits = " + s.n_bits)
    s.columnVector.map(c => c.name -> c.encoder.bits).foreach(println)
    val tups = List(
      Vector(11, 3),
      Vector(512, 2),
      Vector(33, 55)
    )
    tups.map(s.encode_tuple(_).toPaddedString(16)).foreach(println)
    ()
  }

  def combTest() = {
    (10 to 10).map { n =>
      (0 to n).map{ k =>
        val l1 = Profiler("L1"){mk_comb_bi(n, k)}.sorted
        val l2 = Profiler("L2"){comb2(n, k)}.sorted
        println((n, k))
        println("L1 = " + l1)
        println("L2 = " + l2)
      }
    }
    Profiler.print()
  }
  def iowa3() = {
    val name = "Iowa200k"
    val dir = "/Users/sachin/Downloads"
    val cols = Vector(1, 2, 11, 13, 17, 18).map(_ - 1)
    val pathin = s"$dir/$name.csv"
    val pathout = s"$dir/${name}_cols${cols.length}.csv"
    val csvin = CSVReader.read(new FileReader(pathin))

    val csvout = csvin.map(row => cols.map(c => row(c)))
    CSVWriter.write(new FileWriter(pathout), csvout)
    println(csvout.head)

  }

  def investment(): Unit = {

    val sch = new schema.DynamicSchema
    val R = sch.read("investments.json", Some("k_amount"), _.asInstanceOf[Int].toLong)
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
    val n_bits_per_col = 20
    val n_bits = n_cols * n_bits_per_col
    val n_rows = 100 * (1 << n_bits_per_col) + 10 //60 * 1000 * 1000

    val time = new LD2("Time", new StaticNatCol(0, (1<<n_bits_per_col)-2, StaticNatCol.defaultToInt))
    val prod = new LD2("Product", new StaticNatCol(0, (1<<n_bits_per_col)-2, StaticNatCol.defaultToInt))
    val loc = new LD2("Location", new StaticNatCol(0, (1<<n_bits_per_col)-2, StaticNatCol.defaultToInt))
    val sch2 = new StaticSchema2(Vector(time, prod, loc))
    //val columnMap = Map(0 -> "Time", "1" ->)
    //val sch = schema.StaticSchema.mk(n_cols, n_bits_per_col, Map(0 -> "Time", 1 -> "Product", 2 -> "Location"))

    def prefix(c: Int, n: Int) = {
      val res = offset(c, 0 until n)
      println(s"prefix($c $n) = $res")
      res
    }
    def offset(c: Int, vs: Seq[Int]) = vs.reverse.map(n_bits_per_col*(c+1)-1-_).toList
    //--------------CUBE DATA GENERATION-------------
    val vgs = collection.mutable.ArrayBuffer[ValueGenerator]()
    vgs += ConstantValueGenerator(100)
    vgs += RandomValueGenerator(10)
    //vgs += SinValueGenerator(List(0, 1, 2, 3), List(5).map(x => n_bits_per_col + x) ++ List(1).map(x => 2*n_bits_per_col + x), 0, 1, 2)
    vgs += SinValueGenerator(offset(0, List(2, 4, 6)), prefix(1, 1) ++ prefix(2, 1), 0, 1, 50)
    //vgs += SinValueGenerator(List(1, 5, 7), List(11, 12), 0, 1, 8)
    //vgs += SinValueGenerator(List(1, 2, 5), List(20, 22), 0, 1, 8)

    vgs += TrendValueGenerator(prefix(0, 4), prefix(1, 3) ++ prefix(2, 5), 147, 1, 15*256*1)
    vgs += TrendValueGenerator(prefix(0, 2), prefix(1, 2), 3, -1, 100)
    //  //vgs += TrendValueGenerator( List(4, 5, 6, 7), List(), 0, 1, 10)
    // vgs += TrendValueGenerator( prefix(0, 2), prefix(1, 3), 5, 1, 1*3)
    //vgs += TrendValueGenerator(List(2, 3, 4, 5, 6, 7), List(15), 0, -1, 30)
    val vg = SumValueGenerator(vgs)

    val R = ParallelTupleGenerator2(sch2, n_rows, 1000,  Sampling.f1, vg).data
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

  def parPlan() = {
    val m = RandomizedMaterializationScheme(100, 0.1, 1.05)
    println(m.projections.size)
    val seq = m.create_build_plan()
    val par = m.create_parallel_build_plan(8)

    println(seq.map(kv => kv._3 -> kv._2).mkString("", " ", "\n"))

    par.foreach(pseq => println(pseq.map(kv => kv._3 -> kv._2).mkString("", " ", "\n")))
  }

  def sample(n: Int): Unit = {
    val map = collection.mutable.HashMap[Long, Int]() withDefaultValue (0)
    (1 to n).foreach { i =>
      val s = Sampling.f2(1 << 20)
      map(s) += 1
    }
    //map.foreach(println)
    println(map.size)
    map.filter(_._2 > 4).foreach(println)
  }
  def backend_naive() = {
    val n_bits = 10
    val n_rows = 40
    val n_queries = 10
    val query_size = 5
    val rnd = new Random(1L)
    val data = (0 until n_rows).map(i => BigBinary(rnd.nextInt(1<<n_bits)) -> rnd.nextInt(10).toLong)
    val fullcub = CBackend.b.mk(n_bits, data.toIterator)
    println("Full Cuboid data = " + data.mkString("  "))
    val dc = new DataCube(RandomizedMaterializationScheme(n_bits, 0.1, 1.8))
    dc.build(fullcub)
    (0 until n_queries).map{i =>
      val q = Tools.rand_q(n_bits, query_size)
      println("\nQuery =" + q)
      val res = dc.naive_eval(q)
      println("Result = "+res.mkString(" "))
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

    val dc = PartialDataCube.load2(cg.inputname+"_rms2_15_22_0", cg.inputname+"_base")
    val display = FeatureFrameSSB(sf, dc, 50)
  }
  def main(args: Array[String]): Unit = {
    //uniformSolver()
    //prepare()
    //test1()
    //loadtest()
    //combTest()
    //investment()
    //sample(1000)
    //large()
    //feature()
    //parPlan()
    //backend_naive()
    ssb_demo()


  }

}