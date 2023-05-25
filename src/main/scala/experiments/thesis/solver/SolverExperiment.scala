package experiments.thesis.solver

import core.DataCube
import core.solver.SolverTools
import core.solver.lpp.SliceSparseSolver
import experiments.Experiment
import frontend.experiments.Tools
import frontend.generators.{CubeGenerator, NYC, SSB}
import planning.NewProjectionMetaData
import util.Profiler

abstract class SolverExperiment(ename: String, ename2: String = "")(implicit timestampedFolder: String, numIters: Int) extends Experiment(ename2, ename, SolverExperimenter.folder) {
  var runID = 0 //in case same query in given to "run()" multiple times

}


object SolverExperimenter {
  implicit val be = backend.CBackend.default
  val folder = "thesis/solver"

  def main(args: Array[String]) {
    implicit val numIters = 10
    implicit val timestamp = Experiment.now()

    val nyc = new NYC()
    val ssb = new SSB(100)




  }
}