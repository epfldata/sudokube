package frontend.generators

import backend.CBackend
import frontend.schema._
import util.BigBinary
import com.github.tototoshi.csv.CSVReader
import frontend.cubespec.{CompositeMeasure, CountMeasure, Measure, SingleColumnStaticMeasure}
import frontend.schema.encoders.{StaticMemCol, StaticNatCol}

class TinyDataStatic(implicit backend: CBackend) extends MultiCubeGenerator[IndexedSeq[String]]("TinyData") {
  override lazy val schemaInstance = schema()

  val countMeasure = new CountMeasure()
  val valueMeasure = new SingleColumnStaticMeasure(2, "Value", StaticNatCol.defaultToInt(_).get.toLong)
  override val measure =new CompositeMeasure(Vector(countMeasure, valueMeasure))
  override def generatePartitions() = {
    val filename = s"tabledata/TinyData/data.csv"
    val datasize = CSVReader.open(filename).iterator.drop(1).size
    val data = CSVReader.open(filename).iterator.drop(1).map { s =>
      val sIdx = s.toIndexedSeq
      val keys = sIdx.dropRight(1).reverse //for the left-most column to be assigned higher bits
      val encodedKey = schemaInstance.encode_tuple(keys)
      val encodedValue = measure.compute(sIdx)
      //StaticNatCol.floatToInt(2)(value).get.toLong
      encodedKey -> encodedValue
    }
    Vector(datasize -> data)
  }
  override protected def schema(): StaticSchema2 = {
    val quarter = new LD2("Quarter", new StaticMemCol[String](2, (1 to 4).map("Q" + _)))
    val city = new LD2("City", new StaticMemCol[String](2, Vector("Geneva", "Lausanne", "Zurich", "Bern")))
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
