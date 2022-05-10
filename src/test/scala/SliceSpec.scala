import frontend._
import frontend.schema.encoders.StaticNatCol
import org.scalatest.{FlatSpec, Matchers}
import frontend.schema.{LD2, StaticSchema2}


class SliceSpec extends FlatSpec with Matchers {

  import StaticNatCol._

  val c1 = LD2("C1", new StaticNatCol(64, 126, defaultToInt))
  val c2 = LD2("C2", new StaticNatCol(1, 8, defaultToInt))
  val sch = new StaticSchema2(Vector(c1, c2))

  val table = (64 until 125).flatMap { v1 => (1 until 8).map { v2 => sch.encode_tuple(Vector(v1, v2)) -> (v1 * 10 + v2) } }

  "Equality slice" should "work on C1" in {
    val cond1 = SliceEquals(c1.encoder, 70)
    val value1 = 70 * 10 * 7 + 7 * 4
    assert(table.filter(p => cond1.eval(p._1)).map(_._2).sum == value1)
  }

  "Equality slice" should "work on C2" in {
    val cond1 = SliceEquals(c2.encoder, 5)
    val value1 = (125 - 64) * 5 + (124 * 125 - 63 * 64) * 5
    assert(table.filter(p => cond1.eval(p._1)).map(_._2).sum == value1)
  }

  "And slice" should "work" in {
    val cond1 = SliceEquals(c1.encoder, 70)
    val cond2 = SliceEquals(c2.encoder, 5)
    val cond = SliceAnd(cond1, cond2)
    val value = 70 * 10 + 5
    assert(table.filter(p => cond.eval(p._1)).map(_._2).sum == value)
  }

  "Or slice" should "work" in {
    val cond1 = SliceEquals(c1.encoder, 70)
    val cond2 = SliceEquals(c2.encoder, 5)
    val cond = SliceOr(cond1, cond2)
    val value1 = 70 * 10 * 7 + 7 * 4
    val value2 = (125 - 64) * 5 + (124 * 125 - 63 * 64) * 5
    val value3 = 70 * 10 + 5
    val value = value1 + value2 - value3
    assert(table.filter(p => cond.eval(p._1)).map(_._2).sum == value)
  }


  "Range slice" should "throw exception for unaligned values" in {
    //Encoder ranges include NULL values

    val sr10 = SliceRange(c1.encoder, 63, 64)
    assert(sr10.dropBits == 1)
    assert(sr10.i3 == 0)
    assertThrows[IllegalArgumentException](SliceRange(c1.encoder, 64, 64))
    assertThrows[IllegalArgumentException](SliceRange(c1.encoder, 64, 65))
    assertThrows[IllegalArgumentException](SliceRange(c1.encoder, 64, 66))
    assertThrows[IllegalArgumentException](SliceRange(c1.encoder, 64, 67))
    assertThrows[IllegalArgumentException](SliceRange(c1.encoder, 64, 68))


    assertThrows[IllegalArgumentException](SliceRange(c1.encoder, 65, 64))
    assertThrows[IllegalArgumentException](SliceRange(c1.encoder, 65, 65))
    val sr21 = (SliceRange(c1.encoder, 65, 66))
    assert(sr21.dropBits == 1)
    assert(sr21.i3 == 1)
    assertThrows[IllegalArgumentException](SliceRange(c1.encoder, 65, 67))
    assertThrows[IllegalArgumentException](SliceRange(c1.encoder, 65, 68))


    assertThrows[IllegalArgumentException](SliceRange(c1.encoder, 66, 66))
    assertThrows[IllegalArgumentException](SliceRange(c1.encoder, 66, 67))
    assertThrows[IllegalArgumentException](SliceRange(c1.encoder, 66, 68))
    assertThrows[IllegalArgumentException](SliceRange(c1.encoder, 66, 69))
    assertThrows[IllegalArgumentException](SliceRange(c1.encoder, 66, 70))

    assertThrows[IllegalArgumentException](SliceRange(c1.encoder, 67, 67))
    val sr31 = SliceRange(c1.encoder, 67, 68)
    assert(sr31.dropBits == 1)
    assert(sr31.i3 == 2)
    val sr32 = SliceRange(c1.encoder, 67, 70)
    assert(sr32.dropBits == 2)
    assert(sr32.i3 == 1)
  }

  "Range slice" should "work correctly" in {
    val cond1 = SliceRange(c1.encoder, 67, 70)
    val cond2 = SliceRange(c2.encoder, 4, 5)
    val cond = SliceAnd(cond1, cond2)
    val value = 9 * 4 + (70 * 71 - 66 * 67) * 10
    assert(table.filter(p => cond.eval(p._1)).map(_._2).sum == value)
  }

}
