package frontend.generators

import backend.CBackend
import breeze.io.CSVReader
import frontend.JsonReader
import frontend.cubespec.{CompositeMeasure, ConstantMeasure, CountMeasure, Measure, SingleColumnDynamicMeasure, SquareMeasure}
import frontend.schema.DynamicSchema2
import util.{BigBinary, Profiler}

import java.io.{File, FileReader}
import scala.util.Try

class DynamicSchemaCubeGenerator(override val inputname: String,
                                 filename: String,
                                 measure_key: Option[String] = None,
                                 map_value: Object => Long = DynamicSchemaCubeGenerator.defaultToLong
                                )(implicit backend: CBackend) extends CubeGenerator[Map[String, Object]](inputname) {
  type DynamicInput = Map[String, Object]
  override lazy val schemaInstance = schema()
  val measure = if (measure_key.isEmpty) {
    new ConstantMeasure[DynamicInput]("Count", 1L)
  } else {
    new SingleColumnDynamicMeasure(measure_key.get, measure_key.get, map_value)
  }

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
      items.map(l => (schemaInstance.encode_tuple(l.toList), measure.compute(l)))
    }
    else {
      items.map(l => {
        val x = l.toMap
        val measureValue = measure.compute(x)
        (schemaInstance.encode_tuple((x - measure_key.get).toList), measureValue)
      })
    }
    schemaInstance.save(baseName)
    Vector(data.size -> data.toIterator)
  }
  override protected def schema(): DynamicSchema2 = {
    val file = new File("cubedata/" + baseName + "/" + baseName + ".sch")
    if (file.exists())
      DynamicSchema2.load(baseName)
    else
      new DynamicSchema2()
  }
}


class DynamicSchemaMultiCubeGenerator(override val inputname: String,
                                      filename: String,
                                      val measureColNames : IndexedSeq[String],
                                      val measures: IndexedSeq[Measure[Map[String, Object], Long]],
                                     )(implicit backend: CBackend) extends MultiCubeGenerator[Map[String, Object]](inputname) {
  override lazy val schemaInstance = schema()
  override val measure = new CompositeMeasure[Map[String, Object], Long](measures)


  override def generatePartitions() = {
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

    val data = if (measures.isEmpty) {
      items.map(l => (schemaInstance.encode_tuple(l.toList), Vector(1L)))
    }
    else {
      items.map(l => {
        val row = l.toMap
        val measureValues = measure.compute(row)
        (schemaInstance.encode_tuple((row -- measureColNames).toList), measureValues)
      })
    }
    schemaInstance.save(baseName(measure.allNames.head))
    Vector(data.size -> data.toIterator)
  }
  override protected def schema(): DynamicSchema2 = {
    val bn = baseName(measure.allNames.head)
    val file = new File("cubedata/" + bn + "/" + bn + ".sch")
    if (file.exists())
      DynamicSchema2.load(bn)
    else
      new DynamicSchema2()
  }
}

class WebShopDyn(implicit be: CBackend) extends DynamicSchemaCubeGenerator("WebShopDyn", "tabledata/Webshop/salesDyn.json")

class TinyData(implicit be: CBackend) extends DynamicSchemaCubeGenerator("TinyDataDyn", "tabledata/TinyData/data.csv", Some("Value"))

class TinyDataMulti(measureColNames: IndexedSeq[String], measures: IndexedSeq[Measure[Map[String, Object], Long]])(implicit be: CBackend) extends DynamicSchemaMultiCubeGenerator("TinyDataMultiDyn", "tabledata/TinyData/multidata.csv", measureColNames, measures)

object DynamicSchemaCubeGenerator {
  def defaultToLong(v: Object) = v match {
    case s: String => s.toLong
    case _ => v.asInstanceOf[Long]
  }

  def main(args: Array[String]): Unit = {
    implicit val be = CBackend.default
    new WebShopDyn().saveBase()
    new TinyData().saveBase()


    type DynamicInput = Map[String, Object]
    val measures = collection.mutable.ArrayBuffer[Measure[DynamicInput, Long]]()
    val val1 = new SingleColumnDynamicMeasure("Value1", "Value1", defaultToLong)
    val val1Sq = new SquareMeasure(val1)
    val count = new CountMeasure()
    //val cg = new DynamicSchemaMultiCubeGenerator()
  }
}
