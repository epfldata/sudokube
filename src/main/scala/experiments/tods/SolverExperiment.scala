package experiments.tods

import experiments.Experiment

abstract class SolverExperiment(ename: String, ename2: String = "")(implicit timestampedFolder: String, numIters: Int) extends Experiment(ename2, ename, SolverExperimenter.folder) {
  var runID = 0 //in case same query in given to "run()" multiple times

}

object SolverExperimenter {
  implicit val be = backend.CBackend.default
  val folder = "tods23"
}