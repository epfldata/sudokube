//package ch.epfl.data.sudokube
package frontend.schema
import frontend.schema.encoders.{ColEncoder, DynamicColEncoder, MemCol, NatCol}
import util.BigBinaryTools._
import util._

import java.io._
import scala.util.Random


/** A schema that can grow dynamically.
    Whenever we call encode and an element is not there already, add it.

    Example:
    {{{
    $ cat example-data/animals.json
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
    scala> val R = animals_sch.read("example-data/animals.json")
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
class DynamicSchema extends Schema {

  override def n_bits: Int = bitPosRegistry.n_bits

  implicit val bitPosRegistry = new BitPosRegistry
  val columns = collection.mutable.Map[String, ColEncoder[_]]()
  /* protected */
  def columnList = columns.toIndexedSeq

  /** stores integers efficiently using NatCol and a sign bit;
      everything else is represented by MemCol[Option[String]].
  */
  protected def encode_column(key: String, v: Any) =

    if(v.isInstanceOf[Int]) {
      val vi : Int = v.asInstanceOf[Int]

      // create a bit for the sign, where 0 is nonnegative
      val sgn_enc = if(vi < 0) {
        val sgn_key = "-" + key
        val sgn_c = columns.getOrElse(sgn_key, new NatCol())
        columns(sgn_key) = sgn_c
        sgn_c.encode_any(1)
      } else BigBinary(0)

      val c = columns.getOrElse(key,  new NatCol())
      columns(key) = c
      c.encode_any(math.abs(vi)) + sgn_enc
    }
    else {
      val c = columns.getOrElse(key, new MemCol[String](2))
      columns(key) = c
      c.encode_any(v)
    }
  def samplePrefix(total: Int) = {
    import math._
    val count = collection.mutable.HashMap[Int, Int]().withDefaultValue(0)
    var remaining = total
    val cols = columnList
    while(remaining > 0) {
      val prob = Random.nextDouble()
      val factor = if(prob < 0.5)
        1.0
      else if(prob < 0.75)
        0.75
      else if(prob < 0.875)
        0.5
      else
        0.25

      val index  = Random.nextInt(cols.size)
      val nbits = ceil(factor * remaining).toInt min (cols(index)._2.bits.size - count(index))
      count(index) += nbits
      remaining -= nbits
    }
    val bitsCollection = count.map { case (idx, nbits) =>
      val col = cols(idx)._2.asInstanceOf[DynamicColEncoder[_]]
      val bits = Util.collect_n(nbits-1, () => col.bits(Random.nextInt(col.bits.size))).toVector
      //val bits =  col.bits.takeRight(nbits - 1)
       bits:+ col.isNotNullBit
    }
    //println(bitsCollection)
    bitsCollection.reduce(_ ++ _)
  }
  override def decode_dim(q_bits: IndexedSeq[Int]): Seq[Seq[String]] = super.decode_dim(q_bits)
}

class DynamicSchema2 extends Schema2() {
  val root = DynBD2()
  implicit val bitPosRegistry = new BitPosRegistry
  def columnVector: Vector[LD2[_]] = root.children.values.toVector
  override def n_bits: Int = bitPosRegistry.n_bits
  def reset() = {
    bitPosRegistry.reset()
    root.reset()
  }
  def encode_tuple(t: Seq[(String, Any)]): BigBinary = {
    val cols = Profiler("EncodeColumn") { (t.map { case (key, v) => encode_column(key, v) }) }
    Profiler("ColumnSum") { cols.sum }
  }

  /** stores integers efficiently using NatCol and a sign bit;
   *everything else is represented by MemCol[Option[String]].
   */
  protected def encode_column(key: String, v: Any) =

    if (v.isInstanceOf[Int]) {
      val vi: Int = v.asInstanceOf[Int]
      // create a bit for the sign, where 0 is nonnegative
      val sgn_enc = if (vi < 0) {
        val sgn_key = "-" + key
        val sgn_c = root.getOrAdd(sgn_key, new NatCol())
        sgn_c.encode_any(1)
      } else BigBinary(0)

      val c = root.getOrAdd(key, new NatCol())
      c.encode_any(math.abs(vi)) + sgn_enc
    }
    else {
      val c = root.getOrAdd(key, new MemCol[String](2))
      c.encode_any(v)
    }
  def save(filename: String): Unit = {
    val file = new File("cubedata/" + filename + "/" + filename + ".sch")
    if (!file.exists())
      file.getParentFile.mkdirs()
    val oos = new ObjectOutputStream(new FileOutputStream(file))
    oos.writeObject(this)
    oos.close()
  }
}

object DynamicSchema2 {
  def load(filename: String): DynamicSchema2 = {
    val file = new File("cubedata/" + filename + "/" + filename + ".sch")
    val ois = new ObjectInputStream(new FileInputStream(file)) {
      override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
        try {Class.forName(desc.getName, false, getClass.getClassLoader) }
        catch {case ex: ClassNotFoundException => super.resolveClass(desc)}
      }
    }
    val sch = ois.readObject.asInstanceOf[DynamicSchema2]
    ois.close()
    sch
  }
}

