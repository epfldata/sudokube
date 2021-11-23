//package ch.epfl.data.sudokube
package frontend.schema

import frontend.schema.encoders.{ColEncoder, StaticMemCol}
import util.{BigBinary, Profiler}


/** This is a node in a dimension hierarchy, as we are used to in data cubes. */
@SerialVersionUID(5L)
abstract class Dim(val name: String) extends Serializable {
  def n_bits : Int
  def bits : Seq[Int]
  def set_bits(offset: Int)
}


/** A leaf node in the dimension hierarchy */
case class LD[T](
  override val name: String,
  val n_bits: Int,
  vals: Seq[T]
) extends Dim(name)  {
  val encoder = new StaticMemCol[T](n_bits, vals)
  override def bits: Seq[Int] = encoder.bits
  override def set_bits(offset: Int): Unit = encoder.set_bits(offset)
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
  }

  override def bits: Seq[Int] = children.map(_.bits).reduce(_ ++ _)

  def leaves : List[LD[_]] = children.map{ x =>
    if(x.isInstanceOf[BD]) x.asInstanceOf[BD].leaves
    else List(x.asInstanceOf[LD[_]])
  }.flatten
}


/** In a StaticSchema, differently from a DynamicSchema, we cannot add bits to a
    dimension after declaration.

    Example:
    {{{
    object Schema1 extends StaticSchema({
      val year     = LD("Year",     5, 1990 to 2021)
      val quarter  = LD("Quarter",  2, 1 to 4)
      val location = LD("Location", 2,
                        List("Vaud", "Geneva", "Valais", "Fribourg"))
                        // four Swiss cantons
      List(new BD("Time", List(year, quarter)), location)
    })

    scala>  val col = Schema1.columnList.toMap.apply("Year")
    col: frontend.schema.encoders.ColEncoder[_] = LD(Year,5,Range 1990 to 2021)

    scala>  col.bits
    res0: Seq[Int] = Range 0 to 4

    // Take the two highest bits 3 and 4 to split the years into four
    // consecutive ranges.
    scala>  val stuff = col.decode_dim(List(3,4))
    stuff: Seq[Seq[Any]] = Vector(
      Vector(1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997),
      Vector(1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005),
      Vector(2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013),
      Vector(2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021))

    // Take the least significant bit to group even and odd years.
    scala>  val stuff2 = col.decode_dim(List(0))
    stuff2: Seq[Seq[Any]] = Vector(Vector(1990, 1992, 1994, 1996, 1998, 2000, 2002, 2004, 2006, 2008, 2010, 2012, 2014, 2016, 2018, 2020), Vector(1991, 1993, 1995, 1997, 1999, 2001, 2003, 2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019, 2021))
    }}}
*/
class StaticSchema(top_level: List[Dim]) extends Schema {
  /** root of the dimension hierarchy */
  val root = new BD("ROOT", top_level)
  root.set_bits(0)

  /// the sum of the bits in the leaves of the hierarchy.
  val n_bits = root.bits.length

  // the actual columns are the leaves of the hierarchy.
  val columnList = root.leaves.map(c => (c.name, c.encoder))
  val colMap = columnList.toMap

  // uses ColEncoder[T].encode_any
  protected def encode_column(key: String, v: Any) = {
    val encoder = Profiler("ColMap") {colMap(key)}
      Profiler("EncodeAny"){encoder.encode_any(v)}
  }
}


object StaticSchema {
  /** make a flat schema of n_cols columns, each of which has
      n_bits_per_col bits.

      Example:
      {{{
      // 26 non-binary dimensions with 5 bits per dimension
      val Schema2 = StaticSchema.mk(26, 5, (x: Int) => (x + 64).toChar.toString)

      scala>  val col2 = Schema2.columnList(2)
      col2: (String, frontend.schema.encoders.ColEncoder[_]) = (B,LD(B,5,Range 0 to 31))
      }}}
  */
  def mk(n_cols: Int,
         n_bits_per_col: Int = 1,
         name_f: Int => String = (i: Int) => i.toString
  ) = new StaticSchema(
    (0 to n_cols - 1).map(x =>
      LD(name_f(x), n_bits_per_col, 0 to (1 << n_bits_per_col) - 1)).toList
  )
}


