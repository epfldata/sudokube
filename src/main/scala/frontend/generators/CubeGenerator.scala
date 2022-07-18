package frontend.generators
import backend.CBackend
import core.materialization.{MaterializationScheme, OldRandomizedMaterializationScheme}
import core.{DataCube, PartialDataCube}
import frontend.schema.{Schema2, StructuredDynamicSchema}
import util.BigBinary

abstract class CubeGenerator(val inputname: String) {

  def generate() : (StructuredDynamicSchema, Seq[(BigBinary, Long)])
  def generate2() : (Schema2, IndexedSeq[(Int, Iterator[(BigBinary, Long)])]) = ???

  val baseName = inputname + "_base"
  def saveBase() = {
    val (sch, r_its) = generate2()
    sch.initBeforeEncode()
    //println("Recommended (log) parameters for Materialization schema " + sch.recommended_cube)
    val m = MaterializationScheme.only_base_cuboid(sch.n_bits)
    val dc = new DataCube()
    //sch.save(inputname)
    val baseCuboid = CBackend.b.mkParallel(sch.n_bits, r_its)
    dc.build(baseCuboid, m)
    dc.save2(baseName)
    (sch, dc)
  }

  def loadBase() = {
    val sch = schema()
    //sch.columnVector.map(c => c.name -> c.encoder.bits).foreach(println)
    //println("Total = "+sch.n_bits)
    //println("Recommended (log) parameters for Materialization schema " + sch.recommended_cube)
    val dc = DataCube.load2(baseName)
    (sch, dc)
  }
  def load(cubename: String) = {
    val sch = schema()
    val fullname = inputname + "_" + cubename
    val dc = DataCube.load2(fullname)
    (sch, dc)
  }
  def loadPartial(cubename: String) = {
    val sch = schema
    val fullname = inputname + "_" + cubename
    val dc = PartialDataCube.load2(fullname, baseName)
    (sch, dc)
  }

  def multiload(params: Seq[(Double, Double)]) = {
    val sch = StructuredDynamicSchema.load(inputname)
    //sch.columnVector.map(c => c.name -> c.encoder.bits).foreach(println)
    val dcs = params.map{p => p -> loadDC(p._1, p._2)}
    (sch, dcs)
  }
  def load2() = {
    val sch = StructuredDynamicSchema.load(inputname)
    //sch.columnVector.map(c => c.name -> c.encoder.bits).foreach(println)
    val dc = DataCube.load2(s"${inputname}_sch")
    (sch, dc)
  }
  def load(lrf: Double, lbase: Double) = {
    val sch = StructuredDynamicSchema.load(inputname)
    //sch.columnVector.map(c => c.name -> c.encoder.bits).foreach(println)
    //println("Total = "+sch.n_bits)
    val dc = DataCube.load2(s"${inputname}_${lrf}_${lbase}")
    (sch, dc)
  }

  def schema(): Schema2 = ???
  def loadDC(lrf: Double, lbase: Double) = {
    DataCube.load2(s"${inputname}_${lrf}_${lbase}")
  }
}
