package frontend.generators

import backend.CBackend
import frontend.schema._
import util.BigBinary
import com.github.tototoshi.csv.CSVReader
import frontend.schema.encoders.{StaticMemCol, StaticNatCol}

class TinyDataStatic(implicit backend: CBackend)  extends CubeGenerator("TinyData") {
  override lazy val schemaInstance = schema()
  override val measureName: String = "Value"

  override def generatePartitions(): IndexedSeq[(Int, Iterator[(BigBinary, Long)])] = {
    val filename = s"tabledata/TinyData/data.csv"
    val datasize = CSVReader.open(filename).iterator.drop(1).size
    val data = CSVReader.open(filename).iterator.drop(1).map { s =>
      val sIdx = s.toIndexedSeq
      val keys = sIdx.dropRight(1).reverse //for the left-most column to be assigned higher bits
      val value =  sIdx.last
      val encodedKey = schemaInstance.encode_tuple(keys)
       val encodedValue = StaticNatCol.defaultToInt(value).get.toLong
        //StaticNatCol.floatToInt(2)(value).get.toLong
      encodedKey -> encodedValue
    }
    Vector(datasize -> data)
  }
  override protected def schema(): StaticSchema2 = {
    val quarter = new LD2("Quarter", new StaticMemCol[String](2, (1 to 4 ).map("Q"+_)))
    val city = new LD2("City", new StaticMemCol[String](2, Vector("Geneva", "Lausanne",  "Zurich", "Bern")))
    new StaticSchema2(Vector(city, quarter)) //we want city to get lower bits, quarter to get higher bits
  }
}

object TinyDataStatic {
  def main(args: Array[String]) {
    implicit val be = CBackend.default
    val cg = new TinyDataStatic()
    cg.saveBase()
  }
}
