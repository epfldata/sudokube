package frontend.generators

import backend.CBackend
import breeze.io.CSVReader
import frontend.JsonReader
import frontend.schema.DynamicSchema2
import util.{BigBinary, Profiler}

import java.io.{File, FileReader}
import scala.util.Try

class DynamicSchemaCubeGenerator(override val inputname: String,
                                      filename: String,
                                      measure_key: Option[String] = None,
                                      map_value: Object => Long = DynamicSchemaCubeGenerator.defaultToLong
                                     )(implicit backend: CBackend) extends CubeGenerator(inputname) {
  override lazy val schemaInstance = schema()
  override val measureName: String = measure_key.getOrElse("Count")
  override def generatePartitions(): IndexedSeq[(Int, Iterator[(BigBinary, Long)])] = {
    schemaInstance.reset()
    val items = {
      if (filename.endsWith("json"))
        JsonReader.read(filename)
      else if (filename.endsWith("csv")) {

        val csv = Profiler("CSVRead") { CSVReader.read(new FileReader(filename)) }
        val header = csv.head
        val rows = Profiler("AddingColNames") { csv.tail.map(vs => header.zip(vs).toMap) }
        rows
      } else
        throw new UnsupportedOperationException("Only CSV or JSON supported")
    }

    val data = if (measure_key.isEmpty) {
      items.map(l => (schemaInstance.encode_tuple(l.toList), 1L))
    }
    else {
      items.map(l => {
        val x = l.toMap
        val measure = x.get(measure_key.get).map(map_value).getOrElse(0L)
        (schemaInstance.encode_tuple((x - measure_key.get).toList), measure)
      })
    }
    schemaInstance.save(baseName)
    Vector(data.size -> data.toIterator)
  }
  override protected def schema(): DynamicSchema2 = {
    val file = new File("cubedata/" + baseName + "/" + baseName + ".sch")
    if(file.exists())
      DynamicSchema2.load(baseName)
    else
      new DynamicSchema2()
    }
}


class WebShopDyn(implicit be: CBackend) extends DynamicSchemaCubeGenerator("WebShopDyn", "tabledata/Webshop/salesDyn.json")
class TinyData(implicit be: CBackend) extends DynamicSchemaCubeGenerator("TinyData", "tabledata/TinyData/data.csv", Some("Value") )
object DynamicSchemaCubeGenerator {
  def defaultToLong(v: Object) = v match {
    case s: String => s.toLong
    case _ => v.asInstanceOf[Long]
  }

  def main(args: Array[String]): Unit = {
    implicit val be = CBackend.default
    new WebShopDyn().saveBase()
    new TinyData().saveBase()
    val cg = new TinyData()
    cg.schemaInstance.columnVector.foreach(println)

  }
}
