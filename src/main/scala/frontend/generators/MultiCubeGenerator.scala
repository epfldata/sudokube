package frontend.generators

import backend.CBackend
import core.materialization.{MaterializationStrategy, RandomizedMaterializationStrategy, SchemaBasedMaterializationStrategy}
import core.solver.SolverTools
import core.{AbstractDataCube, DataCube, MultiDataCube, PartialDataCube}
import frontend.cubespec.CompositeMeasure

abstract class MultiCubeGenerator[-I](override val inputname: String)(override implicit val backend: CBackend) extends AbstractCubeGenerator[I, IndexedSeq[Long]] {
  type O  = Long
  type OS = IndexedSeq[O]
  val measure: CompositeMeasure[I, O]
  def fullName(mainName: String, subName: String) = mainName + "--" + subName
  def baseName(sname: String) = fullName(inputname + "_base", sname)

  def saveBase() = {
    val r_its = generatePartitions()
    schemaInstance.initBeforeEncode()
    val m = MaterializationStrategy.only_base_cuboid(schemaInstance.n_bits)
    val baseCuboids = backend.mkParallelMulti(schemaInstance.n_bits, measure.ms.size, r_its)
    val dcs = (measure.allNames zip baseCuboids).map { case (sname, basecuboid) =>
      val dc = new DataCube(baseName(sname))
      dc.build(basecuboid, m)
      dc.save()
      dc.primaryMoments = SolverTools.primaryMoments(dc)
      dc.savePrimaryMoments(baseName(sname))
      dc
    }
    new MultiDataCube(dcs)
  }

  def loadBase(generateIfNotExists: Boolean = false) = new MultiDataCube(measure.allNames.map { sname => DataCube.load(baseName(sname)) })

  override def loadPartial(partialName: String): AbstractDataCube[IndexedSeq[O]] = {
    val dcs = measure.allNames.map { sname =>
      PartialDataCube.load(fullName(partialName, sname), baseName(sname))
    }
    new MultiDataCube(dcs)
  }
  override def savePartial(ms: MaterializationStrategy, partialName: String): Unit = {
    measure.allNames.map{ sname  =>
      val dc2 = new PartialDataCube(fullName(partialName, sname), baseName(sname))
      dc2.buildPartial(ms)
      dc2.save()
    }
  }

  def saveRMS(logN: Int, minD: Int, maxD: Int) = {
    val rms = new RandomizedMaterializationStrategy(schemaInstance.n_bits, logN, minD, maxD)
    measure.allNames.map { sname =>
      val rmsname = fullName(s"${inputname}_rms3_${logN}_${minD}_${maxD}", sname)
      val dc2 = new PartialDataCube(rmsname, baseName(sname))
      println(s"Building DataCube $rmsname")
      dc2.buildPartial(rms)
      println(s"Saving DataCube $rmsname")
      dc2.save()
    }
  }

  def loadRMS(logN: Int, minD: Int, maxD: Int) = {
    val dcs = measure.allNames.map { sname =>
      val rmsname = fullName(s"${inputname}_rms3_${logN}_${minD}_${maxD}", sname)
      PartialDataCube.load(rmsname, baseName(sname))
    }
    new MultiDataCube(dcs)
  }

  def saveSMS(logN: Int, minD: Int, maxD: Int) = {
    val sms = new SchemaBasedMaterializationStrategy(schemaInstance, logN, minD, maxD)
    measure.allNames.map { sname =>
      val smsname = fullName(s"${inputname}_sms3_${logN}_${minD}_${maxD}", sname)
      val dc3 = new PartialDataCube(smsname, baseName(sname))
      println(s"Building DataCube $smsname")
      dc3.buildPartial(sms)
      println(s"Saving DataCube $smsname")
      dc3.save()
    }
  }

  def loadSMS(logN: Int, minD: Int, maxD: Int) = {
    val dcs = measure.allNames.map { sname =>
      val smsname = fullName(s"${inputname}_sms3_${logN}_${minD}_${maxD}", sname)
      PartialDataCube.load(smsname, baseName(sname))
    }
    new MultiDataCube(dcs)
  }
}

