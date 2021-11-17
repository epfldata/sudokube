package frontend.generators

import backend.CBackend
import core.{DataCube, RandomizedMaterializationScheme}
import experiments.UniformSolverExpt
import frontend.schema.{BD2, LD2, StructuredDynamicSchema}
import frontend.schema.encoders.{DateCol, MemCol, NestedMemCol, PositionCol}
import util.Profiler
import core.RationalTools._

import java.util.Date

object Iowa {
  def read(name: String) = {
    val date = LD2[Date]("Date", new DateCol(2012))

    val county = LD2[String]("County Number", new MemCol)
    val city = LD2[String]("City", new MemCol)
    val zip = LD2[String]("Zip Code", new MemCol)
    val storeloc = LD2[(Double, Double)]("Store Location", new PositionCol((-96.63, 40.38), 2))
    val store = LD2[String]("Store Number", new MemCol)
    val locDims = BD2("Location", Vector(county, city, zip, storeloc, store), false)

    val item = LD2[Int]("Item Number", new MemCol)
    val category = LD2[String]("Category", new NestedMemCol(s => (s.take(3), s.drop(3))))
    val vendor = LD2[String]("Vendor Number", new MemCol)
    val itemDims = BD2("Item", Vector(item, category, vendor), false)

    val measure = Some("Sale (Dollars)")

    def measureF(s: String) = (s.toDouble * 100).toLong

    val sch = new StructuredDynamicSchema(Vector(date, locDims, itemDims))
    val file = s"/Users/sachin/Downloads/$name.tsv"
    val R = sch.read(file, measure, measureF)
    sch.columnVector.foreach(_.encoder.refreshBits)
    (sch, R)
  }

  def save(inputname: String, lrf: Double, lbase: Double) = {
    val (sch, r) = read(inputname)
    val rf = math.pow(10, lrf)
    val base = math.pow(10, lbase)
    val dc = new DataCube(RandomizedMaterializationScheme(sch.n_bits, rf, base))
    sch.save(inputname)
    dc.build(CBackend.b.mk(sch.n_bits, r.toIterator))
    dc.save2(s"${inputname}_${lrf}_${lbase}")
  }

  def load(inputname: String, lrf: Double, lbase: Double) = {
    val sch = StructuredDynamicSchema.load(inputname)
    sch.columnVector.map(c => c.name -> c.encoder.bits).foreach(println)
    val dc = DataCube.load2(s"${inputname}_${lrf}_${lbase}")
    (sch, dc)
  }

  def main(args: Array[String]) = {
    //save("Iowa200k", -9, 0.15)
    val (sch, dc) = load("Iowa200k", -9, 0.15)
    val qs = sch.queries
    val expt = new UniformSolverExpt(dc)
    qs.filter(x => x.length >= 4  && x.length <= 8).foreach(q => expt.compare(q))
  }
}
