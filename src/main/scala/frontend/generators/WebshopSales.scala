package frontend.generators

import backend.CBackend
import com.github.tototoshi.csv.CSVReader
import frontend.cubespec.Measure
import frontend.schema.encoders.{LazyMemCol, StaticNatCol}
import frontend.schema.{BD2, LD2, Schema2, StaticSchema2}
import util.BigBinary

import java.io.{BufferedInputStream, PrintStream}

class WebshopSales(implicit backend: CBackend)  extends StaticCubeGenerator("WebshopSales") {
  override lazy val schemaInstance = schema()
  override val measure = new Measure[StaticInput, Long] {
    override val name: String = "Price"
    override def compute(sIdx: StaticInput): Long = {
      StaticNatCol.floatToInt(2)(sIdx(15)).get.toLong
    }
  }
  val skipped = Set(0,4,6,10,12,14)
  val notskipped = (0 to 14).toSet.diff(skipped).toVector.sorted
  def genUniq(col: Int): Unit = {
    val filename = s"tabledata/Webshop/sales.csv"
    val data = CSVReader.open(filename).all.tail.map{ s =>
      s(col)
    }.distinct.sorted
    val fout = new PrintStream(s"tabledata/Webshop/uniq/col$col.uniq")
    data.foreach(fout.println)
    fout.close()
  }
  override def generatePartitions(): IndexedSeq[(Int, Iterator[(BigBinary, Long)])] = {
      val filename = s"tabledata/Webshop/sales.csv"
    val datasize = CSVReader.open(filename).iterator.drop(1).size
    val data = CSVReader.open(filename).iterator.drop(1).map { s =>
        val sIdx = s.toIndexedSeq
        schemaInstance.encode_tuple(notskipped.map(i => sIdx(i))) -> measure.compute(sIdx)
      }
    Vector(datasize -> data)
  }
  override protected def schema(): StaticSchema2 = {
    //List(7,8,9,11,13).foreach(genUniq)
    def uniq(i: Int) = s"tabledata/Webshop/uniq/col$i.uniq"
    val year = LD2[Int]("Year", new StaticNatCol(2014, 2015, StaticNatCol.defaultToInt, nullable = false))
    val quarter = LD2[Int]("Quarter", new StaticNatCol(1, 4, StaticNatCol.defaultToInt, nullable = false))
    val month = LD2[Int]("Month", new StaticNatCol(1, 12, StaticNatCol.defaultToInt, nullable = false))
    //skip week
    val day = LD2[Int]("Day", new StaticNatCol(1, 31, StaticNatCol.defaultToInt, nullable = false))
    val timeDims = BD2("Time", Vector(year, quarter, month, day), false)

    val customer = LD2[String]("Customer", new LazyMemCol(uniq(7)))

    val prodCategory = LD2[String]("Category", new LazyMemCol(uniq(8)))
    val productLabel = LD2[String]("Product", new LazyMemCol(uniq(9)))
    val prodDims = BD2("Product", Vector(prodCategory, productLabel), false)

    val continent = LD2[String]("Continent", new LazyMemCol(uniq(11)))
    val country = LD2[String]("Country", new LazyMemCol(uniq(13)))
    val locDims = BD2("Location", Vector(continent, country), false)
    new StaticSchema2(Vector(timeDims, customer, prodDims, locDims))
  }
}

object WebshopSales {
  def main(args: Array[String]) {
    implicit val be = CBackend.default
    val cg = new WebshopSales()
    cg.saveBase()
  }
}
