package core.materialization

import combinatorics.Combinatorics
import frontend.schema.Schema2
import util.Util

/**
 * Materialization scheme that picks cuboids between minD and maxD for a total of approximately 2^{logN} cuboids
 * Starts with logN-1 cuboids at minD
 *
 *
 * @param logND log_2 of total number of cuboids
 * @param maxD  maximum dimension upto which we materialize cuboids
 * @param minD  minimum dimensions upto which we materialize cuboids
 */
abstract class Base2MaterializationScheme(nb: Int, logN: Double, minD: Int, maxD: Int) extends MaterializationScheme(nb) {
  //get number of materialized cuboids with d dims
  def n_proj_d(d: Int) = if (d >= minD && d <= maxD) {
    val n = math.pow(2, logN - 1 + minD - d)
    val c = Combinatorics.comb(n_bits, d).toDouble
    if (n < c) n.toInt else c.toInt
  } else 0

  def getCuboidsForD(d: Int): Vector[IndexedSeq[Int]]

  override val projections = {
    val cubD = (0 until n_bits).flatMap { d =>
      if (d >= minD && d <= maxD) {
        val res = getCuboidsForD(d)
        print(res.length + "/")
        res
      } else {
        print("0/")
        Vector()
      }
    }
    println("1") //for base cuboid
    cubD ++ Vector((0 until n_bits))
  }
  println("Total =" + projections.length)
}


@SerialVersionUID(4L)
class SchemaBasedMaterializationScheme(sch: Schema2, logN: Double, minD: Int, maxD: Int) extends Base2MaterializationScheme(sch.n_bits, logN, minD, maxD) {
  def this(sch: Schema2, logN: Double, minD: Int) = this(sch, logN, minD, (minD + logN - 1).toInt)
  override def getCuboidsForD(d: Int): Vector[IndexedSeq[Int]] = {
    val n_proj = n_proj_d(d)
    (0 until n_proj).map { i => sch.root.samplePrefix(d).toIndexedSeq.sorted }.distinct.toVector
  }
}

/**
 * Same idea as Randomized Materialization Scheme, but parameters are different
 */
@SerialVersionUID(5L)
class RandomizedMaterializationScheme(override val n_bits: Int, logN: Double, minD: Int, maxD: Int) extends Base2MaterializationScheme(n_bits, logN, minD, maxD) {
  def this(n_bits: Int, logN: Double, minD: Int) = this(n_bits, logN, minD, (minD + logN - 1).toInt)
  override def getCuboidsForD(d: Int): Vector[IndexedSeq[Int]] = {
    val n_proj = n_proj_d(d)
    Util.collect_n[IndexedSeq[Int]](n_proj, () =>
      Util.collect_n[Int](d, () =>
        scala.util.Random.nextInt(n_bits)).toIndexedSeq.sorted).toVector
  }
}