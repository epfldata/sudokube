//package ch.epfl.data.sudokube
package frontend.schema
import util._
import util.BigBinaryTools._


/** A schema that can grow dynamically.
    Whenever we call encode and an element is not there already, add it.
*/
class DynamicSchema extends Schema {
  protected var bitpos = 0
  def n_bits = bitpos

  /** manages an expanding collection of global bit indexes. */
  trait RegisterIdx {
    var maxIdx = 0
    var bits : List[Int] = List[Int](bitpos)
    bitpos += 1

    def registerIdx(i: Int) {
      if(i > maxIdx) maxIdx = i

      while(i >= (1 << bits.length)) {
        bits = bits ++ List(bitpos)
        bitpos += 1
      }
    }

  }

  /** The domain of the column is elements of type T and NULL (None). */
  class MemCol[T](init_vals: List[T] = List[T]()
  ) extends ColEncoder[T] with RegisterIdx {

    /* protected */
    var vals = List[T]()
    init_vals.foreach { encode_locally(_) }

    /** returns index in collection vals. */
    def encode_locally(v: T) : Int = {
      val pos = vals.indexWhere(_ == v)
      if(pos >= 0) pos
      else {
        vals = vals ++ List(v)
        registerIdx(vals.length - 1)
        vals.length - 1
      }
    }

    //def decode_locally(i: Int, default: T): T = vals.getOrElse(i, default)
    def decode_locally(i: Int): T = vals(i)
  }

  /** A natural number-valued column.
      The column bits represent the natural number directly, and
      no map needs to be stored.
      NatCol does not have a way or representing NULL values --
      the default value is zero.
  */
  class NatCol extends ColEncoder[Int] with RegisterIdx {
    def encode_locally(v: Int) : Int = { registerIdx(v); v }
    def decode_locally(i: Int) = i
  }


  val columns = collection.mutable.Map[String, ColEncoder[_]]()
  /* protected */
  def columnList = columns.toList

  /** stores integers efficiently using NatCol and a sign bit;
      everything else is represented by MemCol[Option[String]].
  */
  protected def encode_column(key: String, v: Any) = {
    if(v.isInstanceOf[Int]) {
      val vi : Int = v.asInstanceOf[Int]

      // create a bit for the sign, where 0 is nonnegative
      val sgn_enc = if(vi < 0) {
        val sgn_key = "-" + key
        val sgn_c = columns.getOrElse(sgn_key, new NatCol)
        columns(sgn_key) = sgn_c
        sgn_c.encode_any(1)
      } else BigBinary(0)

      val c = columns.getOrElse(key, new NatCol)
      columns(key) = c
      c.encode_any(math.abs(vi)) + sgn_enc
    }
    else {
      val c = columns.getOrElse(key,
        new MemCol[Option[String]](List[Option[String]](None)))
      columns(key) = c
      c.encode_any(Some(v))
    }
  }
}


/*
import frontend.schema._

val sch = new DynamicSchema
val R  = sch.read("tiere.json")

sch.decode_tuple(R(2)._1)
sch.columns("herkunft").decode_dim(List(13))

val q = (sch.columns("herkunft").bits ++ sch.columns("fell.weich").bits).toList

val abc : List[(String, Seq[Seq[Any]])] =
sch.columns.toList.map { case (key, c) => {
  val x = c.decode_dim(q)
  if(x == None) None else Some((key, x.get))
}}.flatten

// show domains of columns
sch.columns.toList.map { case (key, c) => {
  if(c.isInstanceOf[sch.NatCol])
    (key, "Nat(0.." + ((1 << c.bits.length) - 1) + ")")
  else (key, c.asInstanceOf[sch.MemCol[_]].vals.toString)
}}
*/


