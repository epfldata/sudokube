//package ch.epfl.data.sudokube
package frontend.schema
import frontend.schema.encoders.{ColEncoder, MemCol, NatCol}
import util._
import util.BigBinaryTools._


/** A schema that can grow dynamically.
    Whenever we call encode and an element is not there already, add it.

    Example:
    {{{
    $ cat animals.json
    [
      { "name": "polar bear",
        "origin": "North Pole",
        "fur": { "color": "white", "soft": "very" },
        "danger": 10
      },

      { "name": "capybara",
        "origin": "South America",
        "danger": -2,
        "friends": [ { "nordelta": 0 }, { "anaconda": 1 } ]
      }
    ]

    scala> val animals_sch = new DynamicSchema

    // R is the actual data; this is not stored in the schema.
    scala> val R = animals_sch.read("animals.json")
    R: List[(util.BigBinary, Int)] = List(( 10101111,1), (11101 00100000,1))

    scala> val col_names = animals_sch.columnList.map(_._1)
    col_names: List[String] = List(danger, fur.color, friends.1.anaconda,
                         friends.0.nordelta, -danger, name, fur.soft, origin)
    // the -danger bit represents the sign of integer danger, 1 if negative.
    // the 0 in friends.0.nordelta represent the fact that nordelta is the
    // first entry in the friends collection; it does not represent the value.

    scala> val capybara_info = animals_sch.decode_tuple(R(1)._1)
    capybara_info: Seq[(String, Any)] = List((danger,2), (fur.color,None),
      (friends.1.anaconda,1), (friends.0.nordelta,0), (-danger,1),
      (name,Some(capybara)), (fur.soft,None), (origin,Some(South America)))

    // show domains of columns
    scala> val domains = animals_sch.columns.toList.map { case (key, c) => {
      if(c.isInstanceOf[NatCol])
           (key, "Nat(0.." + ((1 << c.bits.length) - 1) + ")")
      else (key, c.asInstanceOf[MemCol[_]].decode_map.toString)
    }}
    domains: List[(String, String)] = List(
      (danger,Nat(0..15)),
      (fur.color,List(None, Some(white))),
      (friends.1.anaconda,Nat(0..1)),
      (friends.0.nordelta,Nat(0..1)),
      (-danger,Nat(0..1)),
      (name,List(None, Some(polar bear), Some(capybara))),
      (fur.soft,List(None, Some(very))),
      (origin,List(None, Some(North Pole), Some(South America))))
    }}}

    See also the documentation for ColEncoder.decode_dim for a continuation of
    this example.
*/
class DynamicSchema extends Schema with BitPosRegistry {

  def n_bits = bitpos

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
        val sgn_c = columns.getOrElse(sgn_key, {val colenc  = new NatCol(); colenc.setRegistry(this); colenc})
        columns(sgn_key) = sgn_c
        sgn_c.encode_any(1)
      } else BigBinary(0)

      val c = columns.getOrElse(key, {val colenc  = new NatCol(); colenc.setRegistry(this); colenc})
      columns(key) = c
      c.encode_any(math.abs(vi)) + sgn_enc
    }
    else {
      val c = columns.getOrElse(key,
        {val colenc = new MemCol[Option[String]]( List[Option[String]](None)); colenc.setRegistry(this); colenc})
      columns(key) = c
      c.encode_any(Some(v))
    }
  }
}



