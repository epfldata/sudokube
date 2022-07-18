package experiments

import core.DataCube
import core.solver.SolverTools
import core.solver.lpp.SliceSparseSolver
import util.{ManualStatsGatherer, Profiler, ProgressIndicator}

import scala.reflect.ClassTag

class LPSolverOnlineExpt[T: Fractional : ClassTag](val ename2: String = "")(implicit shouldRecord: Boolean) extends Experiment("lp-online", ename2) {
  fileout.println("CubeName,SolverName,RunID,QSize,Counter,TimeElapsed(s),DOF,Error,MaxDim,Query,QueryName")
  //println("LP Solver of type " + implicitly[ClassTag[T]])

  var queryCounter = 0
  def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: IndexedSeq[Int]): Unit = {
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
    val prepareList = Profiler("Prepare") {
      dc.index.prepare(q, 30, dc.m.n_bits)
    }
    val iter = prepareList.iterator
    val totalsize = prepareList.size
    //println("Prepare over. #Cuboids to fetch = " + totalsize)
    //Profiler.print()
    val pi = new ProgressIndicator(totalsize)

    while (iter.hasNext && solver.df > 0) {
      val current = iter.next()
      val bits = current.queryIntersection
      if (solver.shouldFetch(bits)) {
        val masklen = current.cuboidCost
        if (masklen > maxDimFetched)
          maxDimFetched = masklen
        val fetched = dc.fetch2(List(current))
        solver.add2(List(bits), fetched)
        solver.gauss(solver.det_vars)
        solver.compute_bounds
        stg.record()
        if (output)
          pi.step
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

