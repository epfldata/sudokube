package experiments

import core.DataCube
import core.solver.SolverTools._
import core.solver.moment.Strategy.CoMoment3
import core.solver.moment.MomentSolverAll
import util.{ManualStatsGatherer, Profiler, ProgressIndicator}

import scala.reflect.ClassTag

class OldMomentSolverOnlineExpt[T: Fractional : ClassTag](val ename2: String = "", containsAllCuboids: Boolean = false)(implicit shouldRecord: Boolean) extends Experiment("moment-online", ename2) {

  fileout.println("CubeName,SolverName,RunID,QSize,Counter,TimeElapsed(s),DOF,Error,MaxDim,Query,QueryName")
  //println("Moment Solver of type " + implicitly[ClassTag[T]])


  override def warmup(nw: Int): Unit = if (!containsAllCuboids) super.warmup(nw) else {
    //Cannot use default warmup because of "containsAllCuboid" set to true
    val dcwarm = DataCube.load2("warmupall")
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
    val s = new MomentSolverAll(q.size, CoMoment3)
    var maxDimFetched = 0
    stg.task = () => (maxDimFetched, s.getStats)
    val prepareList = Profiler("Prepare") { dc.index.prepare(q, 1, dc.m.n_bits) }
    val totalsize = prepareList.size
    val iter = prepareList.iterator
    //println("Prepare over. #Cuboids to fetch = " + totalsize)
    //Profiler.print()
    val pi = new ProgressIndicator(totalsize)
    //l.map(p => (p.accessible_bits, p.mask.length)).foreach(println)
    while (iter.hasNext) {
      val current = iter.next()
      val fetched = Profiler.noprofile("Fetch") {
        dc.fetch2(List(current))
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
        s.fastSolve()
      }
      stg.record()
      if (output)
        pi.step
    }
    stg.finish()

    val naiveRes = dc.naive_eval(q)
    //val naivecum = fastMoments(naiveRes)

    //println("Naive moments")
    //println(naivecum.map(_.toLong).mkString("", " ", "\n"))

    val step = math.max(1, totalsize / 100)
    if (output) {
      stg.stats.foreach { case (time, count, (maxdim, (dof, sol))) =>
        val err = error(naiveRes, sol)
        //if(count % step == 0 || dof < 100 || count < 100)
        //  println(s"$count @ $time : dof=$dof err=$err maxdim=$maxdim")
        fileout.println(s"$dcname,${s.strategy},$queryCounter,${q.size},$count,${time},$dof,$err,$maxdim,$qstr,$qname")
      }
      queryCounter += 1
    }
  }

}
