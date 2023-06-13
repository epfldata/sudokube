package frontend.generators

import backend.CBackend
import core.materialization.{MaterializationStrategy, RandomizedMaterializationStrategy, SchemaBasedMaterializationStrategy}
import core.solver.SolverTools
import core.{AbstractDataCube, DataCube, PartialDataCube}
import frontend.cubespec.Measure
import frontend.schema.Schema2
import util.BigBinary

import java.io.FileNotFoundException

abstract class AbstractCubeGenerator[-I, +O](implicit val backend: CBackend) {
  val inputname: String
  lazy val schemaInstance = schema()
  protected def schema(): Schema2
  def generatePartitions(): IndexedSeq[(Int, Iterator[(BigBinary, O)])]
  val measure: Measure[I, O]
  def loadBase(generateIfNotExists: Boolean = false): AbstractDataCube[O]
  def saveBase(): AbstractDataCube[O]
  def loadPartial(partialName: String): AbstractDataCube[O]
  def loadPartialOrBase(partialName: String) = if(partialName.endsWith("_base")) loadBase() else loadPartial(partialName)
  def savePartial(m: MaterializationStrategy, partialName: String): Unit
  def saveRMS(logN: Int, minD: Int, maxD: Int): Unit
  def loadRMS(logN: Int, minD: Int, maxD: Int): AbstractDataCube[O]
  def saveSMS(logN: Int, minD: Int, maxD: Int): Unit
  def loadSMS(logN: Int, minD: Int, maxD: Int): AbstractDataCube[O]
}

abstract class CubeGenerator[-I](override val inputname: String)(override implicit val backend: CBackend) extends AbstractCubeGenerator[I, Long] {
  lazy val baseName = inputname + "_base"
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
  def loadBase(generateIfNotExists: Boolean = false) = {
    val base = try {
      DataCube.load(baseName)
    } catch {
      case ex: FileNotFoundException => if (generateIfNotExists) saveBase() else throw ex
    }
    assert(schemaInstance.n_bits == base.index.n_bits, s"Schema bits = ${schemaInstance.n_bits} != ${base.index.n_bits} =  bits in base cuboid")
    base
  }
  override def loadPartial(partialName: String): PartialDataCube = PartialDataCube.load(partialName, baseName)

  override def savePartial(ms: MaterializationStrategy, partialName: String): Unit = {
    val dc2 = new PartialDataCube(partialName, baseName)
    dc2.buildPartial(ms)
    dc2.save()
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
}

abstract class StaticCubeGenerator(override val inputname: String)(override implicit val backend: CBackend) extends CubeGenerator[IndexedSeq[String]](inputname) {
  type StaticInput = IndexedSeq[String]
}