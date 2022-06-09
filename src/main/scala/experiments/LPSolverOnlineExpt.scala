package experiments

import core.solver.SliceSparseSolver
import core.{DataCube, SolverTools, SparseSolver}
import util.{ManualStatsGatherer, Profiler, ProgressIndicator}

import scala.reflect.ClassTag

class LPSolverOnlineExpt[T: Fractional : ClassTag](val ename2: String = "")(implicit shouldRecord: Boolean) extends Experiment("lp-online", ename2) {
  fileout.println("CubeName,SolverName,RunID,QSize,Counter,TimeElapsed(s),DOF,Error,MaxDim,Query,QueryName")
  //println("LP Solver of type " + implicitly[ClassTag[T]])

  var queryCounter = 0
  def run(dc: DataCube, dcname: String, qu: Seq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = ""): Unit = {
    val q = qu.sorted
    val qstr = qu.mkString(":")
    //println(s"\nQuery size = ${q.size} \nQuery = " + qu)
    Profiler.resetAll()

    val naiveRes = Profiler("Naive Full") {
      dc.naive_eval(q)
    }

    val stg = new ManualStatsGatherer[(Int, (Int, Int, Double))]()
    stg.start()
    val b1 = SolverTools.mk_all_non_neg(1 << q.size)
    val solver = new SliceSparseSolver[T](q.length, b1, Nil, Nil)
    var maxDimFetched = 0
    stg.task = () => (maxDimFetched, solver.getStats)
    var l = Profiler("Prepare") {
      dc.m.prepare_online_agg(q, 30)
    }
    val totalsize = l.size
    //println("Prepare over. #Cuboids to fetch = " + totalsize)
    //Profiler.print()
    val pi = new ProgressIndicator(l.size)

    while (!(l.isEmpty) && solver.df > 0) {
      val bits = l.head.accessible_bits
      if (solver.shouldFetch(bits)) {
        val masklen = l.head.mask.length
        if (masklen > maxDimFetched)
          maxDimFetched = masklen
        val fetched = dc.fetch2(List(l.head))
        solver.add2(List(bits), fetched)
        solver.gauss(solver.det_vars)
        solver.compute_bounds
        stg.record()
        if (output)
          pi.step
        l = l.tail
      }
      stg.finish()
    }
    val total = naiveRes.sum
    val step = math.max(1, totalsize / 100)
    if (output) {
      stg.stats.foreach { case (time, count, (maxDim, (dof, solved, span))) =>
        val err = span / total
        //if (count % step == 0 || dof < 100 || count < 100)
        //  println(s"$count @ $time : dof=$dof err=$err maxDim=$maxDim")
        fileout.println(s"$dcname,LPSolver,$queryCounter,${q.size},$count,${time},$dof,$err,$maxDim")
      }
      queryCounter += 1
    }
  }
}

