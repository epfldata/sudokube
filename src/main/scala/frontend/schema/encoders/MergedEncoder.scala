package frontend.schema.encoders
import util.{BigBinary, BitUtils}

class MergedMemColEncoder[T](e1: MemCol[T], e2: MemCol[T]) extends ColEncoder[T] {
  val combinedValues = (e1.decode_map ++ e2.decode_map).distinct
  val e1IdxMap = e1.decode_map.map{v => combinedValues.indexOf(v)} //key=i is original encoder becomes key=e1IdxMap(i)
  val e2IdxMap = e2.decode_map.map{v => combinedValues.indexOf(v)}
  val isNotNullBit = e1.isNotNullBit
  override def bits: IndexedSeq[Int] = (e1.bits ++ e2.bits :+ e2.isNotNullBit).sorted //without NULL bit
  override def allBits: IndexedSeq[Int] = bits :+ e1.isNotNullBit
  override def bitsMin: Int = ???
  override def isRange: Boolean = false
  override def encode_locally(v: T): Int = ???
  override def decode_locally(i: Int): T = if(i <= maxIdx) combinedValues(i) else null.asInstanceOf[T]
  override def maxIdx: Int = combinedValues.size - 1
  override def queries(): Set[IndexedSeq[Int]] = ???
  override def encode(v: T): BigBinary = ???
  def getTransformFunction(query: IndexedSeq[Int]) = {
    assert(query.size < 31)
    val e1isNotNullMask = (1 << query.indexOf(e1.isNotNullBit))
    val mergedIsNotNullMask = e1isNotNullMask
    val e2isNotNullMask = (1 << query.indexOf(e2.isNotNullBit))
    val e1bitpos = e1.bits.map(i => 1 << query.indexOf(i)).sum //e1.bits is sorted
    val e2bitpos = e2.bits.map(i => 1 << query.indexOf(i)).sum //e2.bits is sorted
    val allbitspos = e1bitpos + e2bitpos + e2isNotNullMask //exclude e1nullmask

    //val str = "123 Warehousing"
    //        println("E1: " + e1.allBits + "   E2: " + e2.allBits + "   combined: " + allBits)
    //println(s"string $str  e1Index = " + e1.decode_map.indexOf(str) + "   e2Index = " + e2.decode_map.indexOf(str) + "  combined = " + combinedValues.indexOf(str))
    //println(s"allbitpos = " + BitUtils.IntToSet(allbitspos))
    def keyfn(key: Int, value: Double) = {
      val e1IsNull = (key & e1isNotNullMask) == 0
      val e2IsNull = (key & e2isNotNullMask) == 0
      val keyThisColumnPart = key & (allbitspos + e1isNotNullMask)

      val keyLocalE1 = BitUtils.projectIntWithInt(keyThisColumnPart, e1bitpos)
      val keyLocalE2 = BitUtils.projectIntWithInt(keyThisColumnPart, e2bitpos)
      //println(s"key = $key    ${key.toBinaryString} e1Null=$e1IsNull e2Null=$e2IsNull keyE1=$keyLocalE1 keyE2=$keyLocalE2")


      val newColumnPart = if (!e1IsNull) {
        assert(e2IsNull) //there can be key with both not null, but its value MUST be zero
        //Cannot include nullBit in unproject (bits have to be sorted)
        val res = BitUtils.unprojectIntWithInt(e1IdxMap(keyLocalE1) , allbitspos) + mergedIsNotNullMask
        //if(e1.decode_locally(keyLocalE1) == str) {
        //  println(s"key = $key  case E1 keyE1=$keyLocalE1  keyE2=$keyLocalE2  res=$res value=$value")
        //}
        res
      } else if (!e2IsNull) {
        assert(e1IsNull)
        val res = BitUtils.unprojectIntWithInt(e2IdxMap(keyLocalE2) , allbitspos) + mergedIsNotNullMask
        //if (e2.decode_locally(keyLocalE2) == "123 Warehousing") {
        //  println(s"key = $key  case E2 keyE1=$keyLocalE1  keyE2=$keyLocalE2  res=$res=pup(${e2IdxMap(keyLocalE2)})+${mergedIsNotNullMask}  $value")
        //}
        res
      } else {
        0
      }
      key - keyThisColumnPart + newColumnPart
    }
    (res: Array[Double]) => {
      val newres = Array.fill(res.size)(0.0)
      res.indices.foreach { i =>
        if (res(i) > 0) {
          val newi = keyfn(i, res(i))
          newres(newi) += res(i)
        }
      }
      newres
    }
  }

}
