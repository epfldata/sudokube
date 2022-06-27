package experiments

import core.{DataCube, SolverTools}
import core.solver.Strategy._
import core.solver._
import frontend.experiments.Tools
import util.{Profiler, Util}

import scala.util.Random

class MomentSolverCompareBatchExpt(ename2: String = "")(implicit shouldRecord: Boolean) extends Experiment(s"momentcompare-batch", ename2) {
  val sliceValues = Vector(1, 0, 0, 0, 1, 1, 0, 0)

  {
    val header = "CubeName, Query, QSize, SliceValues, SliceSize,\t" +
      "MStrategy, MTotalTime(us), MPrepareTime(us), MFetchTime(us), MSolveTime(us), MErr"
    fileout.println(header)
  }


  override def run(dc: DataCube, dcname: String, qu: Seq[Int], trueResult0: Array[Double], output: Boolean, qname: String): Unit = {
    val qs = qu.size
    val q = qu.sorted
    val aggN = 1 << (qs - sliceValues.length)

    def getSolverStats(stg: Strategy) = {
      Profiler.print()
      val mprep = Profiler.durations(stg + "Moment Prepare")._2 / 1000
      val mfetch = Profiler.durations(stg + "Moment Fetch")._2 / 1000
      val msolve = Profiler.durations(stg + s"Moment Solve")._2 / 1000
      val mtot = Profiler.durations(stg + "Moment Total")._2 / 1000
      s"$stg, $mtot, $mprep, $mfetch, $msolve"
    }

    val cm3limit = 18
    val cm4limit = 21
    Profiler.resetAll()
    val result0 = Profiler(CoMoment3 + "Moment Total") {
      if (qs <= cm3limit) solve(dc, CoMoment3, q, sliceValues) else null
    }
      val s0stats = getSolverStats(CoMoment3)

    Profiler.resetAll()
    val result1 = Profiler(CoMoment4 + "Moment Total") {
      if (qs <= cm4limit) solve(dc, CoMoment4, q, sliceValues) else null
    }
    val s1stats = getSolverStats(CoMoment4)

    Profiler.resetAll()
    val result2 = Profiler(CoMoment5 + "Moment Total") {
      solve(dc, CoMoment5, q, sliceValues)
    }
    val s2stats = getSolverStats(CoMoment5)

    Profiler.resetAll()
    val result3 = Profiler(CoMoment5Slice + "Moment Total") {
      solve(dc, CoMoment5Slice, q, sliceValues)
    }
    val s3stats = getSolverStats(CoMoment5Slice)

    //Profiler.print()
    //println(s"qs = $qs, Total = ${result1.N}  slice = ${1 << sliceValues.length}  agg = $aggN  result1=${result1.solution.length} result2=${result2.solution.length} result3=${result3.solution.length}")
    assert(result3.solution.length == aggN)

    val trueResultSlice = Util.slice(trueResult0, sliceValues)
    val sol0 = if(qs <= cm3limit) Util.slice(result0.solution, sliceValues) else null
    val sol1 = if (qs <= cm4limit) Util.slice(result1.solution, sliceValues) else null
    val sol2 = Util.slice(result2.solution, sliceValues)
    val sol3 = result3.solution
    val err0 = if(qs <= cm3limit) SolverTools.error(trueResultSlice, sol0) else Double.PositiveInfinity
    val err1 = if (qs <= cm4limit) SolverTools.error(trueResultSlice, sol1) else Double.PositiveInfinity
    val err2 = SolverTools.error(trueResultSlice, sol2)
    val err3 = SolverTools.error(trueResultSlice, sol3)
    if (output) {
      val common = s"$dcname, ${qu.mkString(":")}, ${q.size}, ${sliceValues.mkString(":")}, ${sliceValues.length}\t"
      if(qs <= cm3limit) fileout.println(s"$common, $s0stats, $err0")
      if (qs <= cm4limit) fileout.println(s"$common, $s1stats, $err1")
      fileout.println(s"$common, $s2stats, $err2")
      fileout.println(s"$common, $s3stats, $err3")
    }


  }

  def solve(dc: DataCube, strategy: Strategy, q: Seq[Int], sliceValues: IndexedSeq[Int] = Vector()) = {
    type T = Double
    val (l, pm) = Profiler(strategy + "Moment Prepare") {
      dc.m.prepare(q, dc.m.n_bits - 1, dc.m.n_bits - 1) -> SolverTools.preparePrimaryMomentsForQuery[T](q, dc.primaryMoments)
    }
    val maxDimFetch = l.last.mask.length
    //println("Solver Prepare Over.  #Cuboids = "+l.size + "  maxDim="+maxDimFetch)
    val fetched = Profiler(strategy + "Moment Fetch") {
      l.map {
        pm =>
          (pm.accessible_bits, dc.fetch2[T](List(pm)).toArray)
      }
    }
    val result = Profiler(strategy + s"Moment Solve") {
      val s = Profiler(strategy + s"Moment Constructor") {
        strategy match {
          case CoMoment3 => new CoMoment3Solver[T](q.length, true, Moment1Transformer(), pm)
          case CoMoment4 => new CoMoment4Solver[T](q.length, true, Moment1Transformer(), pm)
          case CoMoment5 => new CoMoment5Solver[T](q.length, true, Moment1Transformer(), pm)
          case CoMoment5Slice => new CoMoment5SliceSolver[T](q.length, sliceValues, true, Moment1Transformer(), pm)
        }
      }
      Profiler(strategy + s"Moment Add") {
        fetched.foreach {
          case (bits, array) => s.add(bits, array)
        }
      }
      Profiler(strategy + s"Moment FillMissing") {
        s.fillMissing()
      }
      Profiler(strategy + s"Moment ReverseTransform") {
        s.solve(true)
      }
      s
    }
    result
  }

}
