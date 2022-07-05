package experiments

import core.{Rational, SolverTools, SparseSolver}
import core.solver.SliceSparseSolver
import core.RationalTools._
import core.prepare.Preparer
import util.Profiler

import java.io.PrintStream

object SolverExpt {

  val aggcols = (0 to 1).toList
  val filtcols = List(13, 14, 15, 16, 17, 18, 19, 20)
  val filtvalue = 0
  var readOnlyFullCuboid = false
  var writeSolToFile = false
  def query = aggcols ++ filtcols

  def filteredVars = (0 until (1 << query.length)).filter(cond)

  def cond(id: Int) = (id / (1 << aggcols.length)) == filtvalue

  def getSelectedBounds(s: SparseSolver[Rational]) = {
    val allbounds = s.bounds.zipWithIndex
    allbounds.filter {
      case (r, id) => cond(id)
    }
  }

  def callback(s: SparseSolver[Rational]) = {
    val p1 = Profiler.noprofile("Callback PB"){
    s.propagate_bounds(0 to s.n_vars - 1)
    }
    val selectedbounds = getSelectedBounds(s)

    selectedbounds.foreach { case (r, id) =>
      //val selected = cond(id)
      //println(selected + "   " + id + " :: " + id.toBinaryString + " ==> " + r + " :: " +  r.ub.get.toInt.toBinaryString)
    }
    true
  }

  /**
   * Inlining dc.online_agg with the specified solver
   * @param s
   */
  def run(s: SparseSolver[Rational]): Long = {
    val dc = core.DataCube.load("trend.dc")
    var l = Preparer.default.prepareOnline(dc.m, query, cheap, dc.m.n_bits)

    var df = s.df
    var cont = true
    val startTime = System.nanoTime()
    if(readOnlyFullCuboid) l = List(l.last)
    while ((!l.isEmpty) && (df > 0) && cont) {
      println(l.head.accessible_bits)

      Profiler.noprofile("Add2"){s.add2(List(l.head.accessible_bits), dc.fetch2(List(l.head)))}

      if (df != s.df) { // something added
        val p1 = Profiler.noprofile("GAUSS DET VAR") {
          s.gauss(s.det_vars)
        }
        val p2 = Profiler.noprofile("COMPUTE BOUND") {
          s.compute_bounds
        }

        cont = callback(s)
        df = s.df
      }
      l = l.tail
      Profiler.print()
      Profiler.resetAll()
    }
    val endTime = System.nanoTime()
    (endTime - startTime)/(1000 * 1000)
  }

  val cheap = 20

  val solin = (Rational(-1, 1) :: scala.io.Source.fromFile("sol.txt").getLines().flatMap{
    _.trim.split(" ").map(v => Rational(v.toInt, 1))
  }.toList ).zipWithIndex.map{case (k, v) => v -> k }.toMap

  def main(args: Array[String]) = {

    val b1 = SolverTools.mk_all_non_neg[Rational](1 << query.length)
    val b2 = SolverTools.mk_all_non_neg[Rational](1 << query.length)
    val s1 = SparseSolver(query.length, b1, Nil, Nil)
    val s2 = new SliceSparseSolver(query.length, b2, Nil, Nil, cond)



    var d1 = 0L
    var d2 = 0L
    writeSolToFile = false
    if(writeSolToFile)
    readOnlyFullCuboid = true
    d1 = run(s1)
    println("-------------------------\n--------------------------------\n--------------------------------------")
    d2 = run(s2)



    println(s"Original SparseSolver took $d1 ms")
    println(s"Slice SparseSolver took $d2 ms")

    if(writeSolToFile) {
      val solS1 = s1.bounds.map(_.lb.get).toList
      val solout = new PrintStream("sol.txt")
      //solout.println("-1")
      var cnt = 0
      solS1.foreach { v =>
        cnt += 1
        solout.print(v)
        if (cnt % 10 == 0)
          solout.println
        else solout.print(" ")
      }
      solout.close()
    }


    //assert(solin == solS1)


    //sb1.foreach(println)
    //val sb2 = getSelectedBounds(s2).map(_._1).toList
    //assert(sb1 == sb2)

  }
}
