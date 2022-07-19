package frontend.generators
import backend.CBackend
import core.materialization.{MaterializationScheme, OldRandomizedMaterializationScheme, RandomizedMaterializationScheme, SchemaBasedMaterializationScheme}
import core.solver.SolverTools
import core.{DataCube, PartialDataCube}
import frontend.schema.{Schema2, StructuredDynamicSchema}
import util.BigBinary

abstract class CubeGenerator(val inputname: String) {
  lazy val schemaInstance = schema()
  def generate() : (StructuredDynamicSchema, Seq[(BigBinary, Long)])
  def generate2(): IndexedSeq[(Int, Iterator[(BigBinary, Long)])] = ???

  val baseName = inputname + "_base"
  def saveBase() = {
    val r_its = generate2()
    schemaInstance.initBeforeEncode()
    //println("Recommended (log) parameters for Materialization schema " + sch.recommended_cube)
    val m = MaterializationScheme.only_base_cuboid(schemaInstance.n_bits)
    val dc = new DataCube(baseName)
    //sch.save(inputname)
    val baseCuboid = CBackend.b.mkParallel(schemaInstance.n_bits, r_its)
    dc.build(baseCuboid, m)
    dc.save()
    dc.primaryMoments = SolverTools.primaryMoments(dc)
    dc.savePrimaryMoments(baseName)
    dc
  }
  def loadBase() = {
    DataCube.load(baseName)
  }
  def saveRMS(logN: Int, minD: Int, maxD: Int) = {
    val rms = new RandomizedMaterializationScheme(schemaInstance.n_bits, logN, minD)
    val rmsname = s"${inputname}_rms3_${logN}_${minD}_${maxD}"
    val dc2 = new PartialDataCube(rmsname, baseName)
    println(s"Building DataCube $rmsname")
    dc2.buildPartial(rms)
    println(s"Saving DataCube $rmsname")
    dc2.save()
  }

  def loadRMS(logN: Int, minD: Int, maxD: Int) = {
    val rmsname = s"${inputname}_rms3_${logN}_${minD}_${maxD}"
    PartialDataCube.load(rmsname, baseName)
  }

  def saveSMS(logN: Int, minD: Int, maxD: Int) = {
    val sms = new SchemaBasedMaterializationScheme(schemaInstance, logN, minD)
    val smsname = s"${inputname}_sms3_${logN}_${minD}_${maxD}"
    val dc3 = new PartialDataCube(smsname, baseName)
    println(s"Building DataCube $smsname")
    dc3.buildPartial(sms)
    println(s"Saving DataCube $smsname")
    dc3.save()
  }

  def loadSMS(logN: Int, minD: Int, maxD: Int) = {
    val smsname = s"${inputname}_sms3_${logN}_${minD}_${maxD}"
    PartialDataCube.load(smsname, baseName)
  }
  protected def schema(): Schema2 = ???

}
