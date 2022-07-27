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
import core.solver.iterativeProportionalFittingSolver.{EffectiveIPFSolver, IPFUtils, VanillaIPFSolver}
import core.solver.moment.Strategy._
import core.solver.moment._
import frontend._
import frontend.experiments.Tools
import frontend.generators._
import frontend.gui.{FeatureFrame, FeatureFrameSSB}
import frontend.schema.encoders.StaticNatCol
import frontend.schema.{LD2, StaticSchema2}
import util._
import BitUtils._
import scala.reflect.ClassTag
import scala.util.Random

object DemoTxt {

  /** Example for Moment Solver */
  def momentSolver[T:ClassTag:Fractional]() = {
    val num = implicitly[Fractional[T]]
    //0 and 1D moments are required for MomentSolver that we precompute here
    val pm = List(0 -> 17, 1 -> 4, 2 -> 7, 4 -> 12).map(x => x._1 -> num.fromInt(x._2))
    val total = 3 //total query bits
    val slice = Vector(1, 0)  //slicing for the top k-bits in the order of least significant to most significant
    val agg = total - slice.length //how many bits for aggregation

    val solver = new CoMoment5SliceSolver2[T](total ,slice,true, Moment1Transformer(), pm)
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
    val solver = new VanillaIPFSolver(3, true, actual)
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
      Seq(Seq(0,1), Seq(1,2), Seq(2,3), Seq(0,3,4), Seq(4,5)).map(marginalVariables =>
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

    val solver = new VanillaIPFSolver(6, true, actual)
    val marginalDistributions: Map[Seq[Int], Array[Double]] =
      Seq(Seq(0,1), Seq(1,2), Seq(2,3), Seq(0,3,4), Seq(4,5)).map(marginalVariables =>
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
      Seq(Seq(0,1), Seq(1,2), Seq(2,3), Seq(0,3,4), Seq(4,5)).map(marginalVariables =>
        marginalVariables -> IPFUtils.getMarginalDistribution(6, actual, marginalVariables.size, SetToInt(marginalVariables))
      ).toMap
    implicit def listToInt =SetToInt(_)
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


  /** Demo for running different queries using Naive solver to understand how the backend handles projections */
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
    val sf = 100
    val cg = SSB(sf)
    val logN = 15
    val minD = 14
    val maxD = 30

    /** WARNING: This cube must have been built already. See [[frontend.generators.SSBGen]] **/
    val dc = cg.loadSMS(logN, minD, maxD)
    val display = FeatureFrameSSB(sf, dc, 50)
  }

  def main(args: Array[String]): Unit = {
    momentSolver()
    //backend_naive()
    //ssb_demo()
    //cooking_demo()
    //vanillaIPFSolver()
    //effectiveIPFSolver()
    //vanillaIPFSolver2()
    //momentSolver3()
  }
}