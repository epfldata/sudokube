package frontend

import frontend.schema.encoders.ColEncoder
import util.BigBinary

abstract class SliceCondition {
 def eval(b: BigBinary): Boolean
}

case class SliceEquals[T](e: ColEncoder[T], v: T) extends SliceCondition {
  val i = e.encode_locally(v)
  override def eval(b: BigBinary): Boolean = {
    val bunp =  b.unpup(e.bits).toInt
    i == bunp
  }
}


case class SliceRange[T](e: ColEncoder[T], v1: T, v2: T) extends SliceCondition {
  val i1 = e.encode_locally(v1)
  val i2 = e.encode_locally(v2)
  if(i2 <= i1 || i1 < 0)
    throw new IllegalArgumentException("Not a valid range of size at least 2")
  val dropBits = (math.log(i2-i1+1)/math.log(2)).toInt
  if( !((i2 + 1 == i1 + (1 << dropBits)) && ((i1 & (i2-i1)) == 0)))
    throw new IllegalArgumentException("Range not aligned")

  val b3 = e.bits.drop(dropBits)
  val i3 = i1 >> dropBits
  override def eval(b: BigBinary): Boolean = b.unpup(b3).toInt == i3
}

case class SliceAnd(c1: SliceCondition, c2: SliceCondition) extends SliceCondition {
  override def eval(b: BigBinary): Boolean = c1.eval(b) && c2.eval(b)
}
case class SliceOr(c1: SliceCondition, c2: SliceCondition) extends SliceCondition {
  override def eval(b: BigBinary): Boolean = c1.eval(b) || c2.eval(b)
}