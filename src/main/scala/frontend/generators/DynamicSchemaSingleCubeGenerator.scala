package frontend.generators

import backend.CBackend
import breeze.io.CSVReader
import frontend.JsonReader
import frontend.cubespec.{CompositeMeasure, ConstantMeasure, CountMeasure, Measure, SingleColumnDynamicMeasure, SquareMeasure}
import frontend.schema.{DynamicSchema2, LD2}
import util.{BigBinary, Profiler}

import java.io.{File, FileReader}
import scala.util.Try

trait DynamicCubeGenerator {
  protected def schema(): DynamicSchema2
  val timeDimension: Option[String]
  type DynamicInput = Map[String, Object]
}

class DynamicSchemaSingleCubeGenerator(override val inputname: String,
                                       filename: String,
                                       measure_key: Option[String] = None,
                                       map_value: Object => Long = DynamicSchemaSingleCubeGenerator.defaultToLong,
                                       timelikeDimension: Option[String] = None,
                                      )(implicit backend: CBackend) extends CubeGenerator[Map[String, Object]](inputname) with
  DynamicCubeGenerator {

  override val timeDimension: Option[String] = timelikeDimension
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
    var tupleNum = 0
    val data = if (measure_key.isEmpty) {
      items.map { l =>
        val l2 = if (timeDimension.isDefined && !l.contains(timeDimension.get)) {
          l + (timeDimension.get -> tupleNum.toString)
        } else l
        tupleNum += 1
        (schemaInstance.encode_tuple(l2.toList), measure.compute(l2))
      }
    }
    else {
      items.map(l => {
        val x = if (timeDimension.isDefined && !l.contains(timeDimension.get)) {
          l + (timeDimension.get -> tupleNum.toString)
        } else l
        tupleNum += 1
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
                                      val measureColNames: IndexedSeq[String],
                                      val measures: IndexedSeq[Measure[Map[String, Object], Long]],
                                      timelikeDimension: Option[String] = None,
                                     )(implicit backend: CBackend) extends MultiCubeGenerator[Map[String, Object]](inputname) with DynamicCubeGenerator {
  override lazy val schemaInstance = schema()
  override val measure = new CompositeMeasure[Map[String, Object], Long](measures)
  override val timeDimension: Option[String] = timelikeDimension
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
   var tupleNum = 0
    val data = if (measures.isEmpty) {
      items.map{l =>
        val x = if (timeDimension.isDefined && !l.contains(timeDimension.get)) {
          l + (timeDimension.get -> tupleNum.toString)
        } else l
        tupleNum += 1
        (schemaInstance.encode_tuple(x.toList), Vector(1L))
      }
    }
    else {
      items.map(l => {
        val row = if (timeDimension.isDefined && !l.contains(timeDimension.get)) {
          l + (timeDimension.get -> tupleNum.toString)
        } else l
        tupleNum += 1
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

class WebShopDyn(implicit be: CBackend) extends DynamicSchemaSingleCubeGenerator("WebShopDyn", "tabledata/Webshop/salesDyn.json")

class TinyData(implicit be: CBackend) extends DynamicSchemaSingleCubeGenerator("TinyDataDyn", "tabledata/TinyData/data.csv", Some("Value"))

class TestTinyData(implicit be: CBackend) extends DynamicSchemaSingleCubeGenerator("TestDataDyn", "tabledata/TinyData/test.json", None)

class TinyDataMulti(measureColNames: IndexedSeq[String], measures: IndexedSeq[Measure[Map[String, Object], Long]])(implicit be: CBackend) extends DynamicSchemaMultiCubeGenerator("TinyDataMultiDyn", "tabledata/TinyData/multidata.csv", measureColNames, measures)

object DynamicSchemaSingleCubeGenerator {
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
