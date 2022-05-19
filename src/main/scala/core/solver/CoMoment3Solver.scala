package core.solver

import util.Profiler

class CoMoment3Solver(qsize: Int, batchmode: Boolean, transformer: MomentTransformer, primaryMoments: Seq[(Int, Double)]) extends MomentSolver(qsize, batchmode, transformer, primaryMoments) {
  override val solverName: String = "Comoment3"
  //same for batch and online
  override def fillMissing() = {
    Profiler("FillMissingBatch") {
      momentsToAdd.foreach { case (i, m) =>
        val delta = m - moments(i)
        if(delta != 0.0) {
          moments(i) = m
          val supersets  = Profiler(s"FindSuperSet $solverName") {
            (i + 1 until N).filter(j => (j & i) == i)
          }
          Profiler(s"IncrementSuperSet $solverName") {
            supersets.foreach { j =>
              moments(j) += delta * momentProducts(j - i)
            }
          }
        }
      }
      momentsToAdd.clear()
    }
  }
}
