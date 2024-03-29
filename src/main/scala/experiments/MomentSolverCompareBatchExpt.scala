package experiments

import core.DataCube
import core.solver._
import core.solver.moment.Strategy._
import core.solver.moment.{Strategy => _, _}
import util.{Profiler, Util}

class MomentSolverCompareBatchExpt(ename2: String = "")(implicit shouldRecord: Boolean) extends Experiment(s"momentcompare-batch", ename2) {
  {
    val header = "CubeName, Query, QSize, SliceValues, SliceSize," +
      "MStrategy, MTotalTime(us), MPrepareTime(us), MFetchTime(us), MSolveTime(us), MErr"
    fileout.println(header)
  }


  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult0: Array[Double], output: Boolean, qname: String, sliceValues: IndexedSeq[Int]): Unit = {
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

    val cm3limit = 15
    val cm4limit = 15
    val cm5limit = 15
    Profiler.resetAll()
    var stg = CoMoment3
    val result0 = Profiler(stg + "Moment Total") {
      if (qs <= cm3limit) solve(dc, stg, q, sliceValues) else null
    }
    val s0stats = getSolverStats(stg)

    Profiler.resetAll()
    stg = CoMoment4
    val result1 = Profiler(stg + "Moment Total") {
      if (qs <= cm4limit) solve(dc, stg, q, sliceValues) else null
    }
    val s1stats = getSolverStats(stg)


    Profiler.resetAll()
    stg = CoMoment5
    val result2 = Profiler(stg + "Moment Total") {
     if(qs <= cm5limit) solve(dc, stg, q, sliceValues) else null
    }
    val s2stats = getSolverStats(stg)

    Profiler.resetAll()
    stg = CoMoment5Slice
    val result3 = Profiler(stg + "Moment Total") {
      solve(dc, stg, q, sliceValues)
    }
    val s3stats = getSolverStats(stg)

    Profiler.resetAll()
    stg = CoMoment5Slice2
    val result4 = Profiler(stg + "Moment Total") {
      solve(dc, stg, q, sliceValues)
    }
    val s4stats = getSolverStats(stg)

    Profiler.resetAll()
    stg = CoMoment5Slice3
    val result5 = Profiler(stg + "Moment Total") {
      solve(dc, stg, q, sliceValues)
    }
    val s5stats = getSolverStats(stg)

    //Profiler.print()
    //println(s"qs = $qs, Total = ${result1.N}  slice = ${1 << sliceValues.length}  agg = $aggN  result1=${result1.solution.length} result2=${result2.solution.length} result3=${result3.solution.length}")
    assert(result3.solution.length == aggN)

    val trueResultSlice = Util.slice(trueResult0, sliceValues)
    val sol0 = if (qs <= cm3limit) Util.slice(result0.solution, sliceValues) else null
    val sol1 = if (qs <= cm4limit) Util.slice(result1.solution, sliceValues) else null
    val sol2 = if (qs <= cm5limit) Util.slice(result2.solution, sliceValues) else null
    val sol3 = result3.solution
    val sol4 = result4.solution
    val sol5 = result5.solution
    assert(sol4 sameElements sol5)
    val err0 = if (qs <= cm3limit) SolverTools.error(trueResultSlice, sol0) else Double.PositiveInfinity
    val err1 = if (qs <= cm4limit) SolverTools.error(trueResultSlice, sol1) else Double.PositiveInfinity
    val err2 = if (qs <= cm5limit) SolverTools.error(trueResultSlice, sol2) else Double.PositiveInfinity
    val err3 = SolverTools.error(trueResultSlice, sol3)
    val err4 = SolverTools.error(trueResultSlice, sol4)
    val err5 = SolverTools.error(trueResultSlice, sol5)
    if (output) {
      val common = s"$dcname, ${qu.mkString(":")}, ${q.size}, ${sliceValues.mkString(":")}, ${sliceValues.length}"
      if (qs <= cm3limit) fileout.println(s"$common, $s0stats, $err0")
      if (qs <= cm4limit) fileout.println(s"$common, $s1stats, $err1")
      if (qs <= cm5limit) fileout.println(s"$common, $s2stats, $err2")
      fileout.println(s"$common, $s3stats, $err3")
      fileout.println(s"$common, $s4stats, $err4")
      fileout.println(s"$common, $s5stats, $err5")
    }


  }

  def solve(dc: DataCube, strategy: Strategy, q: IndexedSeq[Int], sliceValues: IndexedSeq[Int] = Vector()) = {
    type T = Double
    val (l, pm) = Profiler(strategy + "Moment Prepare") {
      dc.index.prepareBatch(q) -> SolverTools.preparePrimaryMomentsForQuery[T](q, dc.primaryMoments)
    }
    val maxDimFetch = l.last.cuboidCost
    //println("Solver Prepare Over.  #Cuboids = "+l.size + "  maxDim="+maxDimFetch)
    val fetched = Profiler(strategy + "Moment Fetch") {
      if (strategy != CoMoment5Slice3)
        l.map { pm => (pm.queryIntersection, dc.fetch2[T](List(pm))) }
      else Nil
    }
    val result = Profiler(strategy + s"Moment Solve") {
      val s = Profiler(strategy + s"Moment Constructor") {
        strategy match {
          case CoMoment3 => new CoMoment3Solver[T](q.length, true, Moment1Transformer(), pm)
          case CoMoment4 => new CoMoment4Solver[T](q.length, true, Moment1Transformer(), pm)
          case CoMoment5 => new CoMoment5Solver[T](q.length, true, Moment1Transformer(), pm)
          case CoMoment5Slice => new CoMoment5SliceSolver[T](q.length, sliceValues, true, Moment1Transformer(), pm)
          case CoMoment5Slice2 => new CoMoment5SliceSolver2[T](q.length, sliceValues, true, Moment1Transformer(), pm)
          case CoMoment5Slice3 => new CoMoment5SliceSolver2[T](q.length, sliceValues, true, Moment1Transformer(), pm)
        }
      }
      Profiler(strategy + s"Moment Add") {
        if (strategy == CoMoment5Slice3) {
          s.asInstanceOf[CoMoment5SliceSolver2[T]].fetchAndAdd(l, dc)
        } else
          fetched.foreach { case (bits, array) => s.add(bits, array) }
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
