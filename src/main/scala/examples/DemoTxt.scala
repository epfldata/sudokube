package examples

import backend._
import breeze.linalg.DenseMatrix
import core._
import core.materialization._
import core.solver.RationalTools._
import core.solver.SolverTools.error
import core.solver._
import core.solver.iterativeProportionalFittingSolver.{EffectiveIPFSolver, IPFUtils, VanillaIPFSolver}
import core.solver.moment._
import frontend._
import frontend.experiments.Tools
import frontend.generators._
import frontend.gui.{FeatureFrameSSB, MaterializationView, QueryView}
import frontend.schema.DynamicSchema
import frontend.schema.encoders.{ColEncoder, DynamicColEncoder}
import util.BitUtils._
import util._

import java.io.PrintStream
import scala.reflect.ClassTag
import scala.util.Random

object DemoTxt {

  /** Example for Moment Solver */
  def momentSolver[T:ClassTag:Fractional]() = {
    val num = implicitly[Fractional[T]]
    //0 and 1D moments are required for MomentSolver that we precompute here
    val pm = List(0 -> 17, 1 -> 4, 2 -> 7, 4 -> 12).map(x => x._1 -> num.fromInt(x._2))
    val total = 3 //total query bits
    val slice = Vector(2->1, 1->0).reverse //slicing for the top k-bits in the order of least significant to most significant
    val agg = total - slice.length //how many bits for aggregation

    val solver = new CoMoment5SliceSolver[T](total, slice, true, Moment1Transformer(), pm)
    //val solver = new CoMoment5SliceSolver[T](total ,slice,true, Moment1Transformer(), pm)
    //val solver = new CoMoment5Solver[T](total ,true, Moment1Transformer(), pm)

    //true result
    val actual = Util.slice(Array(0, 1, 3, 1, 7, 2, 3, 0).map(_.toDouble), slice)
    implicit def listToInt = SetToInt(_)

    //Add 2D cuboid containing bits 0,1 to solver
    solver.add(List(0, 1), Array(7, 3, 6, 1).map(x => num.fromInt(x)))

    //Add 2D cuboid containing bits 1,2 to solver
    solver.add(List(1, 2), Array(1, 4, 9, 3).map(x => num.fromInt(x)))

    //Add 2D cuboid containing bits 0,2 to solver
    solver.add(List(0, 2), Array(3, 2, 10, 2).map(x => num.fromInt(x)))

    println("Moments before =" + solver.moments.mkString(" "))
    //extrapolate missing moments
    solver.fillMissing()
    println("Moments after =" + solver.moments.mkString(" "))

    //convert extrapolated moments to values
    val result = solver.solve()
    println(result.map(num.toDouble(_)).mkString(" "))
    println("Error = " + error(actual, result))
  }

  def vanillaIPFSolver(): Unit = { // Bad case for IPF — 2000+ iterations
    val actual = Array(1, 1000, 1000, 1000, 1000, 1000, 1000, 1).map(_.toDouble)
    val solver = new VanillaIPFSolver(3, true, false, actual)
    solver.add(BitUtils.SetToInt(List(0, 1)), Array(1001, 2000, 2000, 1001).map(_.toDouble))
    solver.add(BitUtils.SetToInt(List(1, 2)), Array(1001, 2000, 2000, 1001).map(_.toDouble))
    solver.add(BitUtils.SetToInt(List(0, 2)), Array(1001, 2000, 2000, 1001).map(_.toDouble))
    val result = solver.solve()
    println(result.mkString(" "))
    println("Error = " + error(actual, result))
    solver.verifySolution()
  }

  def effectiveIPFSolver(): Unit = { // Decomposable
    val randomGenerator = new Random()
    val actual: Array[Double] = Array.fill(1 << 6)(0)
    (0 until 1 << 6).foreach(i => actual(i) = randomGenerator.nextInt(100))

    val solver = new EffectiveIPFSolver(6)
    val marginalDistributions: Map[Seq[Int], Array[Double]] =
      Seq(Seq(0, 1), Seq(1, 2), Seq(2, 3), Seq(0, 3, 4), Seq(4, 5)).map(marginalVariables =>
        marginalVariables -> IPFUtils.getMarginalDistribution(6, actual, marginalVariables.size, SetToInt(marginalVariables))
      ).toMap

    marginalDistributions.foreach { case (marginalVariables, clustersDistribution) =>
      solver.add(BitUtils.SetToInt(marginalVariables), clustersDistribution)
    }

    val result = solver.solve()
    println(actual.mkString(" "))
    println(result.mkString(" "))
    println("Error = " + error(actual, result))
    solver.verifySolution()
  }

  def vanillaIPFSolver2(): Unit = { // Decomposable, just for comparison
    val randomGenerator = new Random()
    val actual: Array[Double] = Array.fill(1 << 6)(0)
    (0 until 1 << 6).foreach(i => actual(i) = randomGenerator.nextInt(100))

    val solver = new VanillaIPFSolver(6, true, false, actual)
    val marginalDistributions: Map[Seq[Int], Array[Double]] =
      Seq(Seq(0, 1), Seq(1, 2), Seq(2, 3), Seq(0, 3, 4), Seq(4, 5)).map(marginalVariables =>
        marginalVariables -> IPFUtils.getMarginalDistribution(6, actual, marginalVariables.size, SetToInt(marginalVariables))
      ).toMap

    marginalDistributions.foreach { case (marginalVariables, clustersDistribution) =>
      solver.add(BitUtils.SetToInt(marginalVariables), clustersDistribution)
    }

    val result = solver.solve()
    println(actual.mkString(" "))
    println(result.mkString(" "))
    println("Error = " + error(actual, result))
    solver.verifySolution()
  }

  def momentSolver3(): Unit = { // Decomposable, just for comparison
    val randomGenerator = new Random()
    val actual: Array[Double] = Array.fill(1 << 6)(0)
    (0 until 1 << 6).foreach(i => actual(i) = randomGenerator.nextInt(100))

    val solver = new MomentSolverAll[Rational](6)
    val marginalDistributions: Map[Seq[Int], Array[Double]] =
      Seq(Seq(0, 1), Seq(1, 2), Seq(2, 3), Seq(0, 3, 4), Seq(4, 5)).map(marginalVariables =>
        marginalVariables -> IPFUtils.getMarginalDistribution(6, actual, marginalVariables.size, SetToInt(marginalVariables))
      ).toMap
    implicit def listToInt = SetToInt(_)
    marginalDistributions.foreach { case (marginalVariables, clustersDistribution) =>
      solver.add(marginalVariables, clustersDistribution.map(n => Rational(BigInt(n.toInt), 1)))
    }
    solver.fillMissing()

    val result = solver.fastSolve().map(_.toDouble)
    println(actual.mkString(" "))
    println(result.mkString(" "))
    println("Error = " + error(actual, result))
    solver.verifySolution()
  }

  /** Demo for frontend stuff */
  def cooking_demo(): Unit = {

    val cube = UserCube.createFromJson("example-data/demo_recipes.json", "rating", "demoCube")

    //save/load matrices
    cube.save()
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

    def transformForGroupBy(src: String): String = {
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


  /** Demo for running different queries using Naive solver to understand how the backend handles projections */
  def backend_naive() = {
    val n_bits = 10
    val n_rows = 40
    val n_queries = 10
    val query_size = 5
    val rnd = new Random(1L)
    val data = (0 until n_rows).map(i => BigBinary(rnd.nextInt(1 << n_bits)) -> rnd.nextInt(10).toLong)
    implicit val backend = CBackend.default
    val fullcub = backend.mk(n_bits, data.toIterator)
    println("Full Cuboid data = " + data.mkString("  "))
    val dc = new DataCube()
    val m = new RandomizedMaterializationStrategy(n_bits, 6, 2)
    dc.build(fullcub, m)
    (0 until n_queries).map { i =>
      val q = Tools.rand_q(n_bits, query_size)
      println("\nQuery =" + q)
      val res = dc.naive_eval(q)
      println("Result = " + res.mkString(" "))
    }
  }

  /** GUI demo for SSB */
  def ssb_demo() = {
    implicit val backend = CBackend.default
    val sf = 100
    val cg = SSB(sf)
    val logN = 15
    val minD = 14
    val maxD = 30

    /** WARNING: This cube must have been built already. See [[frontend.generators.SSBGen]] * */
    val dc = cg.loadSMS(logN, minD, maxD)
    val display = FeatureFrameSSB(sf, dc, 50)
  }

  def demo(): Unit = {
    import frontend.schema._
    implicit val be = backend.CBackend.colstore
    val cg = new WebshopSales()
    cg.saveBase()
    val dc = cg.loadBase()
    val sch = cg.schemaInstance
     println("NumRows = " + dc.cuboids.last.size)
    println("NumBits = " + sch.n_bits + "   " + dc.index.n_bits)
    sch.columnVector.foreach{case LD2(n, e) => println(n + " -> " + e.bits.mkString(","))}
    println(dc.naive_eval(Vector(0, 26, 27)).mkString(" "))
    val display = new QueryView(sch, dc)
    val d2 = new MaterializationView()
  }
  def demo2() = {
    implicit val be  = CBackend.rowstore
    val sch = new DynamicSchema
    val data = sch.read("multi7.json" )
    val m = new DynamicSchemaMaterializationStrategy(sch, 10, 5, 15)
    val basename = "multischema"
    val dc = new DataCube(basename)
    val baseCuboid = be.mkAll(sch.n_bits, data)
    dc.build(baseCuboid, m)
    dc.save()
    dc.primaryMoments = SolverTools.primaryMoments(dc)
    dc.savePrimaryMoments(basename)
    println("nbits = " + sch.n_bits)
    println("nrows = " + data.size)
    dc.cuboids.groupBy(_.n_bits).mapValues(_.map(_.numBytes).sum).toList.sortBy(_._1).foreach{println}

    implicit def toDynEncoder[T](c: ColEncoder[T]) = c.asInstanceOf[DynamicColEncoder[T]]
    val alltimebits = sch.columns("ID").bits
    val numallTimeBits = alltimebits.size
    val n = "col1.col8"
    val en = sch.columns(n)
    println("Time bits = " + sch.columns("ID").bits + s"[${sch.columns("ID").isNotNullBit}]")
    //sch.columnList.foreach{ case (n, en) =>
      println("Col " + n + " :: " + en.bits + s"[${en.isNotNullBit}] =  " + (en.bits.size+1) + " bits")
     var continue = true
      var numsliceTimeBits = 0
      val slice  = collection.mutable.ArrayBuffer[(Int, Int)]()
      slice += en.isNotNullBit -> 1
      while(continue) {
        val end =  numallTimeBits-numsliceTimeBits
        val q =  ((end - 1) until end).map(alltimebits(_))
        val slicedims = slice.map(_._1)
        println("Current sliceDimensions = " + slicedims.mkString(" "))
        println("Current sliceValue = " + slice.map(_._2).mkString(" "))
        println("Current query = " + q)
        val qsorted = (q ++ slicedims).sorted
        println("Full query = " + qsorted)
        val qres = dc.naive_eval(qsorted)
        val slice2 = slice.map{case (b, v) => qsorted.indexOf(b) -> v}.sortBy(_._1)

        val list = dc.index.prepareBatch(qsorted)
        list.foreach{pm =>
          val present = BitUtils.IntToSet(pm.queryIntersection).map(i => qsorted(i))
          val missing = qsorted.diff(present)
          println(s"Present = ${present.mkString(" ")}  Missing = ${missing.mkString(" ")}  Cost = ${pm.cuboidCost} #Present=${pm.queryIntersectionSize}")
        }

        val qresslice = Util.slice(qres, slice2)
        //println("True result = " + qres.mkString("  "))
        println("True Slice result = " + qresslice.mkString("  "))


        val fetched = list.map{ pm =>(pm.queryIntersection, dc.fetch2[Double](List(pm)))}

        val ipfsolver = new VanillaIPFSolver(qsorted.size)
        fetched.foreach { case (bits, array) => ipfsolver.add(bits, array) }
        val ipfres = ipfsolver.solve()
        val ipfslice = Util.slice(ipfres, slice2)
        //println("IPF result = " + ipfres.mkString("  "))
        println("IPF Slice result = " + ipfslice.mkString("  "))
        val ipferror = SolverTools.error(qres, ipfres)
        //println("IPF error =" + ipferror)

        val primaryMoments = SolverTools.preparePrimaryMomentsForQuery[Double](qsorted, dc.primaryMoments)
        val momentsolver = new CoMoment5SliceSolver[Double](qsorted.size, slice2, true, new Moment1Transformer, primaryMoments)
        fetched.foreach{ case (bits, array) => momentsolver.add(bits, array) }
        momentsolver.fillMissing()
        val momentres = momentsolver.solve()
        println("Moment Slice result = " + momentres.mkString("  "))

        println("Enter slice bit: ")
        val sbit: Int = scala.io.StdIn.readLine().toInt
        continue = sbit < 2
        slice += q.max -> sbit
        numsliceTimeBits += 1
      }
    //}
  }

  def main(args: Array[String]): Unit = {
    //momentSolver()
    //backend_naive()
    //ssb_demo()
    //cooking_demo()
    //vanillaIPFSolver()
    //effectiveIPFSolver()
    //vanillaIPFSolver2()
    //momentSolver3()
    demo()
  }
}