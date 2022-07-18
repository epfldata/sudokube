package experiments

import core.DataCube
import core.solver.SolverTools._
import core.solver._
import util.{ManualStatsGatherer, Profiler, ProgressIndicator}
import core.solver.moment.Strategy._
import core.solver.moment.{CoMoment3Solver, CoMoment4Solver, Moment1Transformer, MomentSolver}

class NewMomentSolverOnlineExpt(strategy: Strategy, ename2: String = "", containsAllCuboids: Boolean = false)(implicit shouldRecord: Boolean) extends Experiment("newmoment-online", ename2) {

  fileout.println("CubeName,SolverName,RunID,QSize,Counter,TimeElapsed(s),DOF,Error,MaxDim,Query,QueryName,Entropy")

  def solver(qsize: Int, pm: Seq[(Int, Double)])(implicit shouldRecord: Boolean): MomentSolver[Double] = strategy match {
    case CoMoment3 => new CoMoment3Solver(qsize, false, Moment1Transformer(), pm)
    case CoMoment4 => new CoMoment4Solver(qsize, false, Moment1Transformer(), pm)
  }

  override def warmup(nw: Int): Unit = if (!containsAllCuboids) super.warmup(nw) else {
    //Cannot use default warmup because of "containsAllCuboid" set to true
    val dcwarm = DataCube.load2("warmupall")
    dcwarm.loadPrimaryMoments("warmupall")
    (1 until 6).foreach(i => run(dcwarm, "warmupall", 0 until i, null, false, sliceValues = Vector()))
    println("Warmup Complete")
  }

  var queryCounter = 0

  def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: IndexedSeq[Int]): Unit = {
    val q = qu.sorted
    Profiler.resetAll()
    //println(s"\nQuery size = ${q.size} \nQuery = " + qu)
    val qstr = qu.mkString(":")
    val stg = new ManualStatsGatherer[(Int, (Int, Array[Double]))]()
    stg.start()
    val s = solver(q.size, SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments))
    var maxDimFetched = 0
    stg.task = () => ((maxDimFetched, s.getStats))
    val prepareList = Profiler("Prepare") {
      //if (containsAllCuboids)
      //  FullLatticeOnlinePreparer.prepareOnline(dc.m, q, 2, dc.m.n_bits)
      dc.index.prepare(q, 2, dc.m.n_bits)
    }
    val totalsize = prepareList.length
    //println("Prepare over. #Cuboids to fetch = " + totalsize)
    //Profiler.print()
    val pi = new ProgressIndicator(totalsize, "Online aggregation", false)
    val iter = prepareList.iterator
    while (iter.hasNext) {
      val current = iter.next()
      val fetched = Profiler.noprofile("Fetch") {
        dc.fetch2[Double](List(current))
      }

      if (current.cuboidCost > maxDimFetched)
        maxDimFetched = current.cuboidCost
      Profiler.noprofile("Add") {
        s.add(current.queryIntersection, fetched)
      }
      Profiler.noprofile("FillMiss") {
        s.fillMissing()
      }
      Profiler.noprofile("Solve") {
        s.solve()
      }
      stg.record()
      pi.step
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
        fileout.println(s"$dcname,${s.name},$queryCounter,${q.size},$count,${time},$dof,$err,$maxdim,$qstr,$qname,$entr")
      }
      queryCounter += 1
    }
  }

}
