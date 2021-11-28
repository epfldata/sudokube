package experiments

import core.{DataCube, RandomizedMaterializationScheme}
import core.solver.Strategy.CoMomentFrechet
import core.solver.{Strategy, UniformSolver}
import experiments.CubeData.dc
import util.{Profiler, StatsGatherer}

import java.io.PrintStream
import java.time.Instant
import scala.reflect.ClassTag

class UniformSolverOnlineExpt[T:Fractional:ClassTag](dc: DataCube, val name: String = "UniformSolverOnlineExpt") {
  val timestamp = Instant.now().toString
  val lrf = math.log(dc.m.asInstanceOf[RandomizedMaterializationScheme].rf)/math.log(10)
  val lbase = math.log(dc.m.asInstanceOf[RandomizedMaterializationScheme].base)/math.log(10)
  val fileout = new PrintStream(s"expdata/${name}_${timestamp}.csv")
  fileout.println("LogRF,LogBase,Query,QSize, TimeElapsed(ms), DOF, Error")
  println("Uniform Solver of type " + implicitly[ClassTag[T]])


  def error(naive: Array[Double], solver: Array[Double]) = {
    val length = naive.length
    val deviation = (0 until length).map(i => Math.abs(naive(i) - solver(i))).sum
    val sum = naive.sum
    deviation / sum
  }

  def compare(qu: Seq[Int]) = {
    val q = qu.sorted
    println(s"\nQuery size = ${q.size} \nQuery = " + qu)
    val qstr = qu.mkString(":")
    val s = new UniformSolver(q.size, CoMomentFrechet)
    val stg = new StatsGatherer(s.getStats, qstr)
    stg.startManual()
    val cheap_size = 30
    var l = dc.m.prepare_online_agg(q, cheap_size)
    while (!(l.isEmpty) ) {
      val fetched = dc.fetch2(List(l.head))
      val bits = l.head.accessible_bits
      s.add(bits, fetched.toArray)
      s.fillMissing()
      s.fastSolve()
      stg.record()
      l = l.tail
    }
    //stg.finishAuto()

    val naiveRes = dc.naive_eval(q)

    stg.stats.foreach{ case (time, (dof, sol)) =>
    val err =  error(naiveRes, sol)
      println(s"@$time : dof=$dof err=$err")
      fileout.println(s"$lrf,$lbase,$qstr,${q.size},$time,$dof,$err" )
    }
  }

}
