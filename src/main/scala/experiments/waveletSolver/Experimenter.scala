package experiments.waveletSolver

import backend.CBackend
import core.PartialDataCube
import experiments._
import experiments.waveletSolver.Enums.CubeData
import frontend.generators.{AirlineDelay, NYC}

/**
 * Compare the time and error for IPF solvers, Moment solver, and Wavelet solver.
 */
object Experimenter {
  implicit var backend: CBackend = CBackend.colstore
  var backendName = "colstore"

  def main(args: Array[String]): Unit = {
    val maxdim = 40

    implicit val numIters: Int = 1
    implicit val timestampedFolder: String = args.lift(1).getOrElse(Experiment.now())

    import Enums.MaterializationStrategy._

    experiment_qsize(SMS, CubeData.NYC, 15, 18, maxdim)

    //
    //    args.headOption.getOrElse("default") match {
    //      case "NYC-SMS-qsize" => experiment_qsize(SMS, NYC, 15, 18, maxdim)
    //
    //      case "NYC-RMS-qsize" => experiment_qsize(RMS, NYC, 15, 18, maxdim)
    //
    //      case "Airline-SMS-qsize" => experiment_qsize(SMS, Airline, 15, 18, maxdim)
    //
    //      case "Airline-RMS-qsize" => experiment_qsize(RMS, Airline, 15, 18, maxdim)
    //
    //      case "error" => error_analysis()
    //
    //      case s => println(s"Unknown Expt $s with timestamp $timestampedFolder")
    //    }
  }

  def experiment_qsize(matStrat: Enums.MaterializationStrategy, cubeData: Enums.CubeData, logn: Int, minDim: Int, maxDim: Int)
                      (implicit numIters: Int, timestampedFolder: String): Unit = {
    import Enums.{CubeData, MaterializationStrategy}

    val cubeGenerator = cubeData match {
      case CubeData.NYC => NYC()
      case CubeData.Airline => new AirlineDelay()
    }

    val sch = cubeGenerator.schemaInstance

    val ms = matStrat match {
      case MaterializationStrategy.SMS => "sms3"
      case MaterializationStrategy.RMS => "rms3"
    }
    val param = s"${logn}_${minDim}_${maxDim}"

    val name = s"_${ms}_$param"
    val fullname = cubeGenerator.inputname + name

    val dc = PartialDataCube.load(fullname, cubeGenerator.baseName)
    assert(dc.index.n_bits == sch.n_bits)
    dc.loadPrimaryMoments(cubeGenerator.baseName)

    val expname2 = s"query-dim-$cubeGenerator-$ms-dmin-$minDim-$backendName"
    val expt = new Wavelet_IPF_Moment_Batch_Expt(expname2)
    expt.warmup()

    //val materializedQueries = new MaterializedQueryResult(cg)
    val qss = List(8, 10, 12, 14, 16) //, 18)
    qss.foreach { qs =>
      //val queries = materializedQueries.loadQueries(qs).take(numIters)
      val queries = (0 until numIters).map(_ => sch.root.samplePrefix(qs)).distinct

      println(s"QSize stats experiment for ${cubeGenerator.inputname} dataset MS = $ms (d_min = $minDim) Query Dimensionality = $qs")

      val ql = queries.length
      queries.zipWithIndex.foreach { case (q, i) =>
        println(s"\tBatch Query ${i + 1}/$ql")
        //val trueResult = materializedQueries.loadQueryResult(qs, i)
        val trueResult = dc.naive_eval(q.sorted)
        expt.run(dc, fullname, q, trueResult, sliceValues = Vector())
      }
    }

    backend.reset
  }

  def error_analysis()(implicit timestampedFolder: String): Unit = {
    val cg = NYC()
    val sch = cg.schemaInstance

    //val query = sch.root.samplePrefix(15).sorted
    //println(query)
    //val query = Vector(91, 125, 140, 141, 142, 143, 144, 148, 149, 150, 165, 184, 185, 186, 192) //SSB
    val query = Vector(37, 49, 50, 51, 52, 53, 54, 55, 138, 139, 219, 365, 366, 404, 405) //NYC

    val param = s"15_18_40"
    val ms = if (true) "sms3" else "rms3"
    val name = s"_${ms}_$param"
    val fullname = cg.inputname + name

    val dc = PartialDataCube.load(fullname, cg.baseName)
    assert(dc.index.n_bits == sch.n_bits)
    dc.loadPrimaryMoments(cg.baseName)

    val expt = new ErrorAnalysis(cg.inputname)
    val trueResult = dc.naive_eval(query)
    expt.run(dc, dc.cubeName, query, trueResult)
  }

}

object Enums {

  sealed trait CubeData

  sealed trait MaterializationStrategy


  object CubeData {
    case object NYC extends CubeData;

    case object Airline extends CubeData;
  }

  object MaterializationStrategy {
    case object SMS extends MaterializationStrategy;

    case object RMS extends MaterializationStrategy;
  }
}

