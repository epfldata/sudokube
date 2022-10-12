package frontend.generators
import backend.{Backend, CBackend}
import core.materialization.{MaterializationStrategy, RandomizedMaterializationStrategy, SchemaBasedMaterializationStrategy}
import core.solver.SolverTools
import core.{DataCube, PartialDataCube}
import frontend.schema.Schema2
import util.BigBinary

abstract class CubeGenerator(val inputname: String)(implicit val backend: CBackend) {
  lazy val schemaInstance = schema()
  def generatePartitions(): IndexedSeq[(Int, Iterator[(BigBinary, Long)])]

  val baseName = inputname + "_base"
  def saveBase() = {
    val r_its = generatePartitions()
    schemaInstance.initBeforeEncode()
    //println("Recommended (log) parameters for Materialization schema " + sch.recommended_cube)
    val m = MaterializationStrategy.only_base_cuboid(schemaInstance.n_bits)
    val dc = new DataCube(baseName)
    //sch.save(inputname)
    val baseCuboid = backend.mkParallel(schemaInstance.n_bits, r_its)
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
    val rms = new RandomizedMaterializationStrategy(schemaInstance.n_bits, logN, minD, maxD)
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
    val sms = new SchemaBasedMaterializationStrategy(schemaInstance, logN, minD, maxD)
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


  protected def schema(): Schema2

}
