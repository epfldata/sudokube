package examples

import backend._
import breeze.linalg.DenseMatrix
import core._
import core.cube.OptimizedArrayCuboidIndex
import core.ds.settrie.SetTrieForMoments
import core.materialization._
import core.solver.RationalTools._
import core.solver.SolverTools.error
import core.solver._
import core.solver.moment.Strategy._
import core.solver.moment._
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
    val cids = dc.index.asInstanceOf[OptimizedArrayCuboidIndex].zipWithIndex.groupBy(_._1.length).mapValues(_.head).values.toList.sortBy(_._1.length)
    import CBackend.b.{DenseCuboid => DC}
    val trie = new SetTrieForMoments()

    def process(cidcol: (IndexedSeq[Int], Int)) = {
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
    implicit def listToInt = Bits.toInt(_)
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
    implicit def listToInt = Bits.toInt(_)
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



  def investment(): Unit = {

    val sch = new schema.DynamicSchema

    val R = sch.read("example-data/investments.json", Some("k_amount"), _.asInstanceOf[Int].toLong)
    //R.map{case (k, v) => sch.decode_tuple(k).mkString("{",",","}") + "  " + k + " " + v }.foreach(println)

    val basecuboid = CBackend.b.mk(sch.n_bits, R.toIterator)

    val matscheme = new RandomizedMaterializationScheme(sch.n_bits, 8, 4)
    val dc = new DataCube()
    dc.build(basecuboid, matscheme)

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

    val qV = Vector(0, 12) //Company
    val qH = Vector(1) //even or odd years

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


    val cube = UserCube.createFromJson("example-data/demo_recipes.json", "rating")

    //save/load matrices
    cube.save("demoCube")
    val userCube = UserCube.load("demoCube")

    //can query for a matrix
    var matrix = userCube.query(Vector(("price", 1, Nil)), Vector(("time", 3, Nil)), AND, MOMENT, MATRIX).asInstanceOf[DenseMatrix[String]]
    println(matrix.toString(Int.MaxValue, Int.MaxValue) + "\n \n")

    // can add several dimensions, internal sorting
    matrix = userCube.query(Vector(("Region", 2, Nil), ("price", 1, Nil)), Vector(("time", 3, Nil)), AND, MOMENT, MATRIX).asInstanceOf[DenseMatrix[String]]
    println(matrix.toString(Int.MaxValue, Int.MaxValue) + "\n \n")

    //can query for an array, return top/left/values
    val tuple = userCube.query(Vector(("Region", 2, Nil)), Vector(("time", 2, Nil)), AND, MOMENT, ARRAY).asInstanceOf[(Array[Any],
      Array[Any], Array[Any])]
    println(tuple._1.mkString("top header\n(", ", ", ")\n"))
    println(tuple._2.mkString("left header\n(", "\n ", ")\n"))
    println(tuple._3.mkString("values\n(", ", ", ")\n \n"))

  //can query for array of tuples with bit format
    var array = userCube.query(Vector(("Type", 2, Nil), ("price", 2, Nil)), Vector(), AND, MOMENT, TUPLES_BIT).asInstanceOf[Array[
      Any]].map(x => x.asInstanceOf[(String, Any)])
    println(array.mkString("(", "\n ", ")\n \n"))

    //can query for array of tuples with prefix format
    array = userCube.query(Vector(("Type", 2, Nil), ("price", 2, Nil)), Vector(), AND, MOMENT, TUPLES_PREFIX).asInstanceOf[Array[
      Any]].map(x => x.asInstanceOf[(String, Any)])
    println(array.mkString("(", "\n ", ")\n \n"))

    //can slice and dice: select (Type = Dish || Type = Side) && price = cheap
    array = userCube.query(Vector(("Type", 2, List("Dish", "Side")), ("price", 2, List("cheap"))), Vector(), AND, MOMENT, TUPLES_PREFIX).asInstanceOf[Array[
      Any]].map(x => x.asInstanceOf[(String, Any)])
    println(array.mkString("(", "\n ", ")\n \n"))

    //select (Type = Dish || Type = Side) || price = cheap
    array = userCube.query(Vector(("Type", 2, List("Dish", "Side")), ("price", 2, List("cheap"))), Vector(), OR, MOMENT, TUPLES_PREFIX).asInstanceOf[Array[
      Any]].map(x => x.asInstanceOf[(String, Any)])
    println(array.mkString("(", "\n ", ")\n \n"))
    //delete zero tuples
    array = ArrayFunctions.deleteZeroColumns(array)
    println(array.mkString("(", "\n ", ")\n \n"))

    array = userCube.query(Vector(("Type", 0, Nil)), Vector(), OR, MOMENT, TUPLES_PREFIX).asInstanceOf[Array[
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


  def backend_naive() = {
    val n_bits = 10
    val n_rows = 40
    val n_queries = 10
    val query_size = 5
    val rnd = new Random(1L)
    val data = (0 until n_rows).map(i => BigBinary(rnd.nextInt(1 << n_bits)) -> rnd.nextInt(10).toLong)
    val fullcub = CBackend.b.mk(n_bits, data.toIterator)
    println("Full Cuboid data = " + data.mkString("  "))
    val dc = new DataCube()
    val m = new RandomizedMaterializationScheme(n_bits, 6, 2)
    dc.build(fullcub, m)
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
    //cooking_demo()
    //trieCube()
    //toTrie()
  }
}