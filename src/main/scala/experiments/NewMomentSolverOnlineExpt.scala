package experiments

import core.{DataCube, SolverTools}
import core.SolverTools._
import core.solver.{CoMoment4Solver, Moment1Transformer, MomentSolver}
import util.{ManualStatsGatherer, Profiler, ProgressIndicator}


abstract class NewMomentSolverOnlineExpt(ename2: String = "", containsAllCuboids: Boolean = false)(implicit shouldRecord: Boolean) extends Experiment("newmoment-online", ename2) {

  fileout.println("Name,RunID,QSize,Counter,TimeElapsed(s),DOF,Error,MaxDim,Query,Entropy")

  def solver(qsize: Int, pm: Seq[(Int, Double)]) : MomentSolver
  override def warmup(nw: Int): Unit = if (!containsAllCuboids) super.warmup(nw) else {
    //Cannot use default warmup because of "containsAllCuboid" set to true
    val dcwarm = DataCube.load2("warmupall")
    dcwarm.loadPrimaryMoments("warmupall")
    (1 until 6).foreach(i => run(dcwarm, "warmupall", 0 until i, false))
    println("Warmup Complete")
  }

  var queryCounter = 0

  def run(dc: DataCube, dcname: String, qu: Seq[Int], output: Boolean = true): Unit = {
    val q = qu.sorted
    Profiler.resetAll()
    //println(s"\nQuery size = ${q.size} \nQuery = " + qu)
    val qstr = qu.mkString(":")
    val s = solver(q.size, SolverTools.preparePrimaryMomentsForQuery(q, dc.primaryMoments))
    var maxDimFetched = 0
    val stg = new ManualStatsGatherer((maxDimFetched, s.getStats))
    stg.start()

    var l = Profiler("Prepare") {
      if (containsAllCuboids)
        dc.m.prepare_online_full(q, 2)
      else
        dc.m.prepare_online_agg(q, 2)
    }
    val totalsize = l.size
    //println("Prepare over. #Cuboids to fetch = " + totalsize)
    //Profiler.print()
    val pi = new ProgressIndicator(l.size, "Online aggregation", output)
    //l.map(p => (p.accessible_bits, p.mask.length)).foreach(println)
    while (!(l.isEmpty)) {
      val fetched = Profiler.noprofile("Fetch") {
        dc.fetch2[Double](List(l.head))
      }
      val bits = l.head.accessible_bits
      if (l.head.mask.length > maxDimFetched)
        maxDimFetched = l.head.mask.length
      Profiler.noprofile("Add") {
        s.add(bits, fetched.toArray)
      }
      Profiler.noprofile("FillMiss") {
        s.fillMissing()
      }
      Profiler.noprofile("Solve") {
        s.solve()
      }
      stg.record()
      pi.step
      l = l.tail
    }
    stg.finish()

    val naiveRes = dc.naive_eval(q)

    val step = math.max(1, totalsize / 100)
    if (output) {
      stg.stats.foreach { case (time, count, (maxdim, (dof, sol))) =>
        val err = error(naiveRes, sol)
        val entr = entropy(sol)
        //if(count % step == 0 || dof < 100 || count < 100)
        //  println(s"$count @ $time : dof=$dof err=$err maxdim=$maxdim")
        fileout.println(s"$dcname,$queryCounter,${q.size},$count,${time},$dof,$err,$maxdim,$qstr,$entr")
      }
      queryCounter += 1
    }
  }

}

class CoMoment4OnlineExpt(ename2: String = "", containsAllCuboids: Boolean = false)(implicit shouldRecord: Boolean) extends NewMomentSolverOnlineExpt(ename2, containsAllCuboids) {
  override def solver(qsize: Int, pm: Seq[(Int, Double)]): MomentSolver = new CoMoment4Solver(qsize, false, Moment1Transformer, pm)
}
