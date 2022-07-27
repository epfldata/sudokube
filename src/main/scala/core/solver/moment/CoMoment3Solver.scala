package core.solver.moment

import util.Profiler

import scala.reflect.ClassTag

class CoMoment3Solver[T:ClassTag:Fractional](qsize: Int, batchmode: Boolean, transformer: MomentTransformer[T], primaryMoments: Seq[(Int, T)]) extends MomentSolver(qsize, batchmode, transformer, primaryMoments) {
  override val solverName: String = "Comoment3"
  //same for batch and online
  override def fillMissing() = {
    Profiler("FillMissingBatch") {
      momentsToAdd.foreach { case (i, m) =>
        val delta = num.minus(m, moments(i))
        if(delta != num.zero) {
          moments(i) = m
          val supersets  = Profiler(s"FindSuperSet $solverName") {
            (i + 1 until N).filter(j => (j & i) == i)
          }
          Profiler(s"IncrementSuperSet $solverName") {
            supersets.foreach { j =>
              moments(j) = num.plus(moments(j),  num.times(delta, momentProducts(j - i)))
            }
          }
        }
      }
      momentsToAdd.clear()
    }
  }
}
