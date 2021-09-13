//package ch.epfl.data.sudokube
package frontend.schema

import util.{BigBinary, Profiler}


abstract class Dim(val name: String) extends Serializable {
  def n_bits : Int
  var bits : Seq[Int] = List[Int]()
  def set_bits(offset: Int)
}


/** A leaf node in the dimension hierarchy */
case class LD[T](
  override val name: String,
  val n_bits: Int,
  vals: Seq[T]
) extends Dim(name) with ColEncoder[T] {

  def set_bits(offset: Int) { bits = (offset to offset + n_bits - 1) }

  private val vanity_map: Map[T, Int] =
    vals.zipWithIndex.map{ case (k, i) => (k, i) }.toMap
  private val reverse_map: Map[Int, T] =
    vals.zipWithIndex.map{ case (k, i) => (i, k) }.toMap

  override def encode(v: T): BigBinary = if(v.isInstanceOf[Int] && n_bits == 1) {
    val vi = v.asInstanceOf[Int]
    Profiler("LD Encode") {
      val res1 = if (vi == 0)
        BigBinary(0)
      else
        BigBinary(BigInt(1) << bits.head)
      //val res2 =  super.encode(v)
      //assert(res1 == res2)
      res1
    }
  } else super.encode(v)

  def encode_locally(key: T) : Int = vanity_map(key)
  def decode_locally(i: Int) : T   = reverse_map(i)

  def maxIdx = (1 << bits.length) - 1
}


/** A branch node in the dimension hierarchy */
class BD(override val name: String, children: List[Dim]
) extends Dim(name) {

  def n_bits = children.map(_.n_bits).sum
  def set_bits(offset: Int) {
    val end = children.foldLeft(offset){
      case (off, c) => {
        c.set_bits(off)
        off + c.n_bits
      }
    }
    bits = (offset to end - 1)
  }

  def leaves : List[LD[_]] = children.map{ x =>
    if(x.isInstanceOf[BD]) x.asInstanceOf[BD].leaves
    else List(x.asInstanceOf[LD[_]])
  }.flatten
}


class StaticSchema(top_level: List[Dim]) extends Schema {
  /** root of the dimension hierarchy */
  val root = new BD("ROOT", top_level)
  root.set_bits(0)

  val n_bits = root.bits.length
  val columnList = root.leaves.map(c => (c.name, c))
  val colMap = columnList.toMap

  protected def encode_column(key: String, v: Any) = {
    val encoder = Profiler("ColMap") {colMap(key)}
      Profiler("EncodeAny"){encoder.encode_any(v)}
  }
}


object StaticSchema {
  /** make a flat schema of n_cols columns, each of which has
      n_bits_per_col bits.
  */
  def mk(n_cols: Int,
         n_bits_per_col: Int = 1,
         name_f: Int => String = (i: Int) => i.toString
  ) = new StaticSchema(
    (0 to n_cols - 1).map(x =>
      LD(name_f(x), n_bits_per_col, 0 to (1 << n_bits_per_col) - 1)).toList
  )
}


/*
object Schema1 extends StaticSchema({
    val year     = LD("Year",     5, 1990 to 2021)
    val quarter  = LD("Quarter",  2, 1 to 4)
    val location = LD("Location", 2,
                      List("Vaud", "Geneva", "Valais", "Fribourg"))
    List(new BD("Time", List(year, quarter)), location)
})

Schema1.columnList.toMap.apply("Year").decode_dim(List(3,4))


// 26 non-binary dimensions with 5 bits per dimension
// val Schema2 = StaticSchema.mk(26, 5, (x: Int) => (x + 64).toChar.toString)
*/


