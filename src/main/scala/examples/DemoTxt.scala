package examples

import backend._
import breeze.linalg.DenseMatrix
import combinatorics.Combinatorics.comb
import core.RationalTools._
import core.SolverTools._
import core._
import core.materialization._
import core.solver.Strategy._
import core.solver._
import frontend._
import frontend.experiments.Tools
import frontend.generators._
import frontend.gui.{FeatureFrame, FeatureFrameSSB}
import frontend.schema.encoders.StaticNatCol
import frontend.schema.{LD2, StaticSchema2}
import util._

import scala.reflect.ClassTag
import scala.util.Random

object DemoTxt {

  def trieCube(): Unit = {
    val dc = PartialDataCube.load2("SSB-sf100_sms3_15_14_30", "SSB-sf100_base")
    val cids = dc.m.projections.zipWithIndex.groupBy(_._1.length).mapValues(_.head).values.toList.sortBy(_._1.length)
    import CBackend.b.{DenseCuboid => DC}
    import core.solver.Moment1Transformer
    val trie = new SetTrieForMoments()

    def process(cidcol: (List[Int], Int)) = {
      val cols = cidcol._1
      val cid = cidcol._2
      val n = cols.length
      val cuboid =  dc.cuboids(cid)
      assert(cuboid.n_bits == n)
      val dcub = Profiler(s"Rehash $n") {cuboid.rehash_to_dense(Array.fill(n)(1))}
      val fetched = Profiler(s"Fetch $n") { dcub.asInstanceOf[DC].fetch.map(_.sm)}
      val moments = Profiler(s"Moments $n") { Moment1Transformer[Double]().getMoments(fetched)}
      Profiler(s"TrieInsert $n") {trie.insertAll(cols, moments)}
      Profiler.print()
    }
    cids.foreach{ process(_) }
  }
  def toTrie() = {
    val name = "SSB-sf100_sms3_15_14_30"
    val dc = PartialDataCube.load2(name, "SSB-sf100_base")
    dc.saveAsTrie(name)
  }

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

  def momentSolver2[T:ClassTag:Fractional]() = {
    val num = implicitly[Fractional[T]]
    val pm = List(0 -> 17, 1 -> 4, 2 -> 7, 4 -> 12).map(x => x._1 -> num.fromInt(x._2))
    val total = 3
    val slice = Vector(1, 0, 1)
    val agg = total - slice.length
    //val solver = new CoMoment5SliceSolver2[T](total ,slice,true, Moment1Transformer(), pm)
    val solver = new CoMoment5SliceSolver[T](total ,slice,true, Moment1Transformer(), pm)
    //val solver = new CoMoment5Solver[T](total ,true, Moment1Transformer(), pm)
    val actual = Util.slice(Array(0, 1, 3, 1, 7, 2, 3, 0).map(_.toDouble), slice)
    solver.add(List(0, 1), Array(7, 3, 6, 1).map(x => num.fromInt(x)))
    solver.add(List(1, 2), Array(1, 4, 9, 3).map(x => num.fromInt(x)))
    solver.add(List(0, 2), Array(3, 2, 10, 2).map(x => num.fromInt(x)))
    println("Moments before =" + solver.moments.mkString(" "))
    solver.fillMissing()
    println("Moments after =" + solver.moments.mkString(" "))
    val result = solver.solve()
    println(result.mkString(" "))
    println("Error = " + error(actual, result.toArray))

  }

  def test() = {
    val data = (0 to 15).map(i => BigBinary(i) -> i.toLong)
    val nbits = 10
    val dc = new DataCube(OldRandomizedMaterializationScheme(nbits, 1, 1))
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

    val matscheme = new RandomizedMaterializationScheme(sch.n_bits, 8, 4)
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

  def cooking_demo(): Unit = {


    val cube = UserCube.createFromJson("demo_recipes.json", "rating")

    //save/load matrices
    cube.save("demoCube")
    val userCube = UserCube.load("demoCube")

    //can query for a matrix
    var matrix = userCube.query(List(("price", 1, Nil)), List(("time", 3, Nil)), AND, MOMENT, MATRIX).asInstanceOf[DenseMatrix[String]]
    println(matrix.toString(Int.MaxValue, Int.MaxValue) + "\n \n")

    // can add several dimensions, internal sorting
    matrix = userCube.query(List(("Region", 2, Nil), ("price", 1, Nil)), List(("time", 3, Nil)), AND, MOMENT, MATRIX).asInstanceOf[DenseMatrix[String]]
    println(matrix.toString(Int.MaxValue, Int.MaxValue) + "\n \n")

    //can query for an array, return top/left/values
    val tuple = userCube.query(List(("Region", 2, Nil)), List(("time", 2, Nil)), AND, MOMENT, ARRAY).asInstanceOf[(Array[Any],
      Array[Any], Array[Any])]
    println(tuple._1.mkString("top header\n(", ", ", ")\n"))
    println(tuple._2.mkString("left header\n(", "\n ", ")\n"))
    println(tuple._3.mkString("values\n(", ", ", ")\n \n"))

  //can query for array of tuples with bit format
    var array = userCube.query(List(("Type", 2, Nil), ("price", 2, Nil)), Nil, AND, MOMENT, TUPLES_BIT).asInstanceOf[Array[
      Any]].map(x => x.asInstanceOf[(String, Any)])
    println(array.mkString("(", "\n ", ")\n \n"))

    //can query for array of tuples with prefix format
    array = userCube.query(List(("Type", 2, Nil), ("price", 2, Nil)), Nil, AND, MOMENT, TUPLES_PREFIX).asInstanceOf[Array[
      Any]].map(x => x.asInstanceOf[(String, Any)])
    println(array.mkString("(", "\n ", ")\n \n"))

    //can slice and dice: select (Type = Dish || Type = Side) && price = cheap
    array = userCube.query(List(("Type", 2, List("Dish", "Side")), ("price", 2, List("cheap"))), Nil, AND, MOMENT, TUPLES_PREFIX).asInstanceOf[Array[
      Any]].map(x => x.asInstanceOf[(String, Any)])
    println(array.mkString("(", "\n ", ")\n \n"))

    //select (Type = Dish || Type = Side) || price = cheap
    array = userCube.query(List(("Type", 2, List("Dish", "Side")), ("price", 2, List("cheap"))), Nil, OR, MOMENT, TUPLES_PREFIX).asInstanceOf[Array[
      Any]].map(x => x.asInstanceOf[(String, Any)])
    println(array.mkString("(", "\n ", ")\n \n"))
    //delete zero tuples
    array = ArrayFunctions.deleteZeroColumns(array)
    println(array.mkString("(", "\n ", ")\n \n"))

    array = userCube.query(List(("Type", 0, Nil)), Nil, OR, MOMENT, TUPLES_PREFIX).asInstanceOf[Array[
      Any]].map(x => x.asInstanceOf[(String, Any)])
    println(array.mkString("(", "\n ", ")\n \n"))

    //can apply some binary function
    println(ArrayFunctions.applyBinary(array, binaryFunction, ("price", "Type"), EXIST))
    println(ArrayFunctions.applyBinary(array, binaryFunction, ("price", "Type"), FORALL) + "\n \n")

    def binaryFunction(str1: Any, str2: Any): Boolean = {
      str1.toString.equals("cheap") && !str2.toString.equals("Dish")
    }

    def transformForGroupBy(src : String): String = {
      src match {
        case "Europe" | "Italy" | "France" => "European"
        case _ => "Non-European"
      }
    }

    //can query another dimension, with TUPLES_PREFIX format
    println(userCube.queryDimension(("time", 4, Nil), "difficulty", MOMENT))
    println(userCube.queryDimension(("difficulty", 4, Nil), null, MOMENT) + "\n \n")

    //can detect double peaks and monotonicity
    println(userCube.queryDimensionDoublePeak(("time", 4, Nil), "difficulty", MOMENT, 0.0))
    println(userCube.queryDimensionDoublePeak(("difficulty", 4, Nil), null, MOMENT, 0.0))

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
    val dc = new DataCube(new RandomizedMaterializationScheme(n_bits, 6, 2))
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
    cooking_demo()
    //trieCube()
    //toTrie()
  }
}