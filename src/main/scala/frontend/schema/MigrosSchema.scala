package frontend.schema
import frontend.schema.encoders.ColEncoder
import util.BigBinary
/*
case class MigrosSchema(prod_bits: Int, time_bits: Int, loc_bits: Int) extends Schema {
  override def n_bits: Int = prod_bits + time_bits + loc_bits

  case class SimpleIntEncoder(offset: Int,  length: Int) extends ColEncoder[Int] {
    assert(length <= 31)
    override def bits: Seq[Int] = (offset until offset + length)
    override def encode_locally(v: Int): Int = v
    override def decode_locally(i: Int): Int = i
    override def maxIdx: Int = ((1L << length)-1).toInt
  }
  val colMap = List("Time" -> time_bits, "Location" -> loc_bits, "Product" -> prod_bits).
    foldLeft((List[(String, SimpleIntEncoder)](), 0)){
      case ((res, offset), (name, length)) =>
        val encoder = name -> SimpleIntEncoder(offset, length)
        (encoder :: res, offset + length)
    }._1.toMap

  override def columnList: List[(String, ColEncoder[_])] = colMap.toList
  override protected def encode_column(key: String, v: Any): BigBinary = colMap(key).encode_any(v)
}
*/