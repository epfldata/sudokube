package experiments

import core.{Rational, SolverTools, SparseSolver}
import core.solver.SliceSparseSolver
import core.RationalTools._
import javafx.scene.control.ProgressIndicator

object SolverExpt {
  type T = Rational
  val aggcols = (4 to 7).toList
  val filtcols = List(13, 14, 15, 20, 21, 22, 23)
  val filtvalue = 0

  def query = aggcols ++ filtcols

  def filteredVars = (0 until (1 << query.length)).filter(cond)

  def cond(id: Int) = (id / (1 << aggcols.length)) == filtvalue

  def getSelectedBounds(s: SparseSolver[T]) = {
    val allbounds = s.bounds.zipWithIndex
    allbounds.filter {
      case (r, id) => cond(id)
    }
  }

  def callback(s: SparseSolver[T]) = {
    s.propagate_bounds(0 to s.n_vars - 1)
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
  def run(s: SparseSolver[T]) = {
    val dc = core.DataCube.load("trend.dc")
    var l = dc.m.prepare_online_agg(query, cheap)

    var df = s.df
    var cont = true
    while ((!l.isEmpty) && (df > 0) && cont) {
      println(l.head.accessible_bits)
      s.add2(List(l.head.accessible_bits), dc.fetch2(List(l.head)))
      if (df != s.df) { // something added
        s.gauss(s.det_vars)
        s.compute_bounds
        cont = callback(s)
        df = s.df
      }
      l = l.tail
    }
  }

  val cheap = 20

  def main(args: Array[String]) = {

    val b1 = SolverTools.mk_all_non_neg[T](1 << query.length)
    val b2 = SolverTools.mk_all_non_neg[T](1 << query.length)
    val s1 = SparseSolver(query.length, b1, Nil, Nil)
    val s2 = new SliceSparseSolver(query.length, b2, Nil, Nil, cond)
    )


    //run(s1)
    println("-------------------------\n--------------------------------\n--------------------------------------")
    run(s2)


    //println("val sol = List(-1,")
    //var cnt = 0
    //s1.bounds.map(_.lb.get).foreach { v =>
    //  cnt += 1
    //  print(v+",")
    //  if(cnt % 10 == 0)
    //    println
    //}
    //println(")")


    //sb1.foreach(println)
    //val sb2 = getSelectedBounds(s2).map(_._1).toList
    //assert(sb1 == sb2)

  }
}
