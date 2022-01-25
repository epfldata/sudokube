package experiments

import core.SolverTools.fastMoments
import core.{DataCube, RandomizedMaterializationScheme}
import core.solver.Strategy.{CoMoment3, CoMomentFrechet, MeanProduct}
import core.solver.{Strategy, UniformSolver}
import experiments.CubeData.dc
import util.{AutoStatsGatherer, ManualStatsGatherer, Profiler, ProgressIndicator}

import java.io.{File, PrintStream}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.reflect.ClassTag

class UniformSolverOnlineExpt[T:Fractional:ClassTag](dc_expt: DataCube, val name: String = "", containsAllCuboids: Boolean = false)(implicit  shouldRecord: Boolean) extends Experiment(dc_expt, "US-Online", name) {

  fileout.println("Name,Query,QSize,Counter,TimeElapsed(s),DOF,Error")
  println("Uniform Solver of type " + implicitly[ClassTag[T]])


  def error(naive: Array[Double], solver: Array[Double]) = {
    val length = naive.length
    val deviation = (0 until length).map(i => Math.abs(naive(i) - solver(i))).sum
    val sum = naive.sum
    deviation / sum
  }

  def run(qu: Seq[Int], output: Boolean = true): Unit = {
    val q = qu.sorted
    Profiler.resetAll()
    println(s"\nQuery size = ${q.size} \nQuery = " + qu)
    val qstr = qu.mkString(":")
    val s = new UniformSolver(q.size, CoMoment3)
    var maxDimFetched = 0
    val stg = new ManualStatsGatherer((maxDimFetched, s.getStats))
    stg.start()

    var l = Profiler("Prepare"){
      if(containsAllCuboids)
      dc.m.prepare_online_full(q, 1)
      else
      dc.m.prepare_online_agg(q, 1)
    }
    val totalsize = l.size
    println("Prepare over. #Cuboids to fetch = " + totalsize)
    Profiler.print()
    val pi = new ProgressIndicator(l.size)
    //l.map(p => (p.accessible_bits, p.mask.length)).foreach(println)
    while (!(l.isEmpty) ) {
      val fetched = Profiler.noprofile("Fetch"){dc.fetch2(List(l.head))}
      val bits = l.head.accessible_bits
      if(l.head.mask.length > maxDimFetched)
        maxDimFetched = l.head.mask.length
      Profiler.noprofile("Add"){s.add(bits, fetched.toArray)}
      Profiler.noprofile("FillMiss"){s.fillMissing()}
      Profiler.noprofile("Solve"){s.fastSolve()}
      stg.record()
      pi.step
      l = l.tail
    }
    stg.finish()

    val naiveRes = dc.naive_eval(q)
    //val naivecum = fastMoments(naiveRes)

    //println("Naive moments")
    //println(naivecum.map(_.toLong).mkString("", " ", "\n"))

    val step = math.max(1, totalsize/100)
    if(output) {
      stg.stats.foreach { case (time, count, (maxdim, (dof, sol))) =>
        val err = error(naiveRes, sol)
        if(count % step == 0 || dof < 100 || count < 100)
          println(s"$count @ $time : dof=$dof err=$err maxdim=$maxdim")
        fileout.println(s"$name,$qstr,${q.size},$count,${time},$dof,$err,$maxdim")
      }
    }
  }

}
