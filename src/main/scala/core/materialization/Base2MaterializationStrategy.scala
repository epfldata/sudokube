package core.materialization

import combinatorics.Combinatorics
import frontend.schema.{DynamicSchema, Schema2}
import util.Util

/**
 * Materialization strategy that picks cuboids between minD and maxD for a total of approximately 2^{logN} cuboids
 * Starts with logN-1 cuboids at minD
 *
 *
 * @param logND log_2 of total number of cuboids
 * @param maxD  maximum dimension upto which we materialize cuboids
 * @param minD  minimum dimensions upto which we materialize cuboids
 */
abstract class Base2MaterializationStrategy(nb: Int, logN: Double, minD: Int, maxD: Int) extends MaterializationStrategy(nb) {
  //get number of materialized cuboids with d dims
  def n_proj_d(d: Int) = if (d >= minD && d <= maxD && d <= (logN - 1 + minD)) {
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
/**
 * Concrete subclass of [[Base2MaterializationStrategy]] that picks cuboids of a given dimensionality as prefixes of bits
 * from cosmetic dimensions in the schema
 */
class SchemaBasedMaterializationStrategy(sch: Schema2, logN: Double, minD: Int, maxD: Int) extends Base2MaterializationStrategy(sch.n_bits, logN, minD, maxD) {
  def this(sch: Schema2, logN: Double, minD: Int) = this(sch, logN, minD, (minD + logN - 1).toInt)
  override def getCuboidsForD(d: Int): Vector[IndexedSeq[Int]] = {
    val n_proj = n_proj_d(d)
    (0 until n_proj).map { i => sch.root.samplePrefix(d).toIndexedSeq.sorted }.distinct.toVector
  }
}

/**
 * Concrete subclass of [[Base2MaterializationStrategy]] that picks cuboids of a given dimensionality randomly
 */
@SerialVersionUID(5L)
class RandomizedMaterializationStrategy(override val n_bits: Int, logN: Double, minD: Int, maxD: Int) extends Base2MaterializationStrategy(n_bits, logN, minD, maxD) {
  def this(n_bits: Int, logN: Double, minD: Int) = this(n_bits, logN, minD, (minD + logN - 1).toInt)
  override def getCuboidsForD(d: Int): Vector[IndexedSeq[Int]] = {
    val n_proj = n_proj_d(d)
    Util.collect_n[IndexedSeq[Int]](n_proj, () =>
      Util.collect_n[Int](d, () =>
        scala.util.Random.nextInt(n_bits)).toIndexedSeq.sorted).toVector
  }
}

case class DynamicSchemaMaterializationStrategy(sch: DynamicSchema, logN: Double, minD: Int, maxD: Int) extends Base2MaterializationStrategy(sch.n_bits, logN, minD, maxD) {
  def this(sch: DynamicSchema, logN: Double, minD: Int) = this(sch, logN, minD, (minD + logN - 1).toInt)
  override def getCuboidsForD(d: Int): Vector[IndexedSeq[Int]] = {
    val n_proj = n_proj_d(d)
    //println(s"Dimensionality ${d}")
    Util.collect_n[IndexedSeq[Int]](n_proj, () => sch.samplePrefix(d).sorted).toVector
  }
}