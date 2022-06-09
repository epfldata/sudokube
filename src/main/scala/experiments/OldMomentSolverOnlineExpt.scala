package experiments

import core.SolverTools._
import core.{DataCube, RandomizedMaterializationScheme, SolverTools}
import core.solver.Strategy.{CoMoment3, CoMomentFrechet, MeanProduct}
import core.solver.{Strategy, MomentSolverAll}
import util.{AutoStatsGatherer, ManualStatsGatherer, Profiler, ProgressIndicator}

import java.io.{File, PrintStream}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.reflect.ClassTag

class OldMomentSolverOnlineExpt[T: Fractional : ClassTag](val ename2: String = "", containsAllCuboids: Boolean = false)(implicit shouldRecord: Boolean) extends Experiment("moment-online", ename2) {

  fileout.println("CubeName,SolverName,RunID,QSize,Counter,TimeElapsed(s),DOF,Error,MaxDim,Query,QueryName")
  //println("Moment Solver of type " + implicitly[ClassTag[T]])


  override def warmup(nw: Int): Unit = if (!containsAllCuboids) super.warmup(nw) else {
    //Cannot use default warmup because of "containsAllCuboid" set to true
    val dcwarm = DataCube.load2("warmupall")
    (1 until 6).foreach(i => run(dcwarm, "warmupall", 0 until i, null, false))
    println("Warmup Complete")
  }

  var queryCounter = 0

  def run(dc: DataCube, dcname: String, qu: Seq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = ""): Unit = {
    val q = qu.sorted
    Profiler.resetAll()
    //println(s"\nQuery size = ${q.size} \nQuery = " + qu)
    val qstr = qu.mkString(":")
    val stg = new ManualStatsGatherer[(Int, (Int, Array[Double]))]()
    stg.start()
    val s = new MomentSolverAll(q.size, CoMoment3)
    var maxDimFetched = 0
    stg.task = () => (maxDimFetched, s.getStats)
    var l = Profiler("Prepare") {
      if (containsAllCuboids)
        dc.m.prepare_online_full(q, 1)
      else
        dc.m.prepare_online_agg(q, 1)
    }
    val totalsize = l.size
    //println("Prepare over. #Cuboids to fetch = " + totalsize)
    //Profiler.print()
    val pi = new ProgressIndicator(l.size)
    //l.map(p => (p.accessible_bits, p.mask.length)).foreach(println)
    while (!(l.isEmpty)) {
      val fetched = Profiler.noprofile("Fetch") {
        dc.fetch2(List(l.head))
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
        s.fastSolve()
      }
      stg.record()
      if (output)
        pi.step
      l = l.tail
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
