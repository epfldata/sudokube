package experiments

import backend.CBackend
import core.DataCube
import core.solver._
import core.solver.moment.Strategy._
import core.solver.moment.{Strategy => _, _}
import util.{Profiler, Util}

class MomentSolverCompareBatchExpt(ename2: String = "", subfolder: String)(implicit timestampedFolder: String) extends Experiment(s"momentcompare-batch", ename2, subfolder) {
  {
    val header = "CubeName, Query, QSize, SliceValues, SliceSize," +
      "MStrategy, MTotalTime(us), MPrepareTime(us), MFetchTime(us), MSolveTime(us), " +
      "TrueUnslicedTotal, TrueSliceTotal, SolverSliceTotal, TotalDeviation, MErr,  " +
      "MaxDevTrueValue, MaxDevSolverValue, MaxDev, MaxNormalizedDev"
    fileout.println(header)
  }
  type T = Double

  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult0: Array[Double], output: Boolean, qname: String, sliceValues: Seq[(Int, Int)]): Unit = {
    val qs = qu.size
    val q = qu.sorted
    val aggN = 1 << (qs - sliceValues.length)

    def getSolverStats(stg: Strategy) = {
      //Profiler.print()
      val mprep = Profiler.durations(stg + "Moment Prepare")._2 / 1000
      val mfetch = Profiler.durations(stg + "Moment Fetch")._2 / 1000
      val msolve = Profiler.durations(stg + s"Moment Solve")._2 / 1000
      val mtot = Profiler.durations(stg + "Moment Total")._2 / 1000
      s"$stg, $mtot, $mprep, $mfetch, $msolve"
    }

    Profiler.resetAll()

    var stg = CoMoment3

    Profiler.resetAll()
    stg = CoMoment5
    val result2 = Profiler(stg + "Moment Total") {
      solve(dc, stg, q, sliceValues)
    }
    val s2stats = getSolverStats(stg)


    Profiler.resetAll()
    stg = CoMoment5Slice
    val result4 = Profiler(stg + "Moment Total") {
      solve(dc, stg, q, sliceValues)
    }
    val s4stats = getSolverStats(stg)

    Profiler.resetAll()
    stg = CoMoment5SliceTrieStore
    val result5 = Profiler(stg + "Moment Total") {
      val ts = CBackend.triestore
      val slice2 = Array.fill[Int](qs)(-1)
      sliceValues.foreach { case (i, v) => slice2(i) = v }
      val pm = Profiler(stg + "Moment Fetch") { SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments) }
      Profiler(stg + "Moment Solve") {
        val s = new CoMoment5SliceSolverDouble(qs, sliceValues, true, Moment1Transformer(), pm)
        s.moments = ts.prepareFromTrie(q, slice2)
        s.fillMissing()
        s.solve(true)
        s
      }
    }
    val s5stats = getSolverStats(stg)


    val trueResultSlice = Util.slice(trueResult0, sliceValues)
    val sol2 = Util.slice(result2.solution, sliceValues)
    val sol4 = result4.solution
    val sol5 = result5.solution
    val fulltotal = trueResult0.sum

    val err2 = SolverTools.errorPlus(trueResultSlice, sol2)
    val err4 = SolverTools.errorPlus(trueResultSlice, sol4)
    val err5 = SolverTools.errorPlus(trueResultSlice, sol5)
    if (output) {
      val common = s"$dcname, ${qu.mkString(":")}, ${q.size}, ${sliceValues.map(x => x._1 + ":" + x._2).mkString(";")}, ${sliceValues.length}"
      fileout.println(s"$common, $s2stats, $fulltotal, $err2")
      fileout.println(s"$common, $s4stats, $fulltotal, $err4")
      fileout.println(s"$common, $s5stats, $fulltotal, $err5")
    }


  }

  def solve(dc: DataCube, strategy: Strategy, q: IndexedSeq[Int], slice: Seq[(Int, Int)] = Vector()) = {

    val (l, pm) = Profiler(strategy + "Moment Prepare") {
      dc.index.prepareBatch(q) -> SolverTools.preparePrimaryMomentsForQuery[T](q, dc.primaryMoments)
    }
    val maxDimFetch = l.last.cuboidCost
    //println("Solver Prepare Over.  #Cuboids = "+l.size + "  maxDim="+maxDimFetch)
    val fetched = Profiler(strategy + "Moment Fetch") {
      l.map { pm => (pm.queryIntersection, dc.fetch2[T](List(pm))) }
    }

    val result = Profiler(strategy + s"Moment Solve") {
      val s = Profiler(strategy + s"Moment Solve.Constructor") {
        strategy match {
          case CoMoment3 => new CoMoment3Solver[T](q.length, true, Moment1Transformer(), pm)
          case CoMoment4 => new CoMoment4Solver[T](q.length, true, Moment1Transformer(), pm)
          case CoMoment5 => new CoMoment5SolverDouble(q.length, true, Moment1Transformer(), pm)
          case CoMoment5Slice => new CoMoment5SliceSolverDouble(q.length, slice, true, Moment1Transformer(), pm)
        }
      }
      Profiler(strategy + s"Moment Solve.Add") {
        fetched.foreach { case (bits, array) => s.add(bits, array) }
      }
      Profiler(strategy + s"Moment Solve.FillMissing") {
        s.fillMissing()
      }
      Profiler(strategy + s"Moment Solve.ReverseTransform") {
        s.solve(true)
      }
      s
    }
    result
  }

}
