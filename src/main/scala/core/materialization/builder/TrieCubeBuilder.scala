package core.materialization.builder
import core.SetTrieBuildPlan
import core.materialization.MaterializationScheme
import util.ProgressIndicator

object TrieCubeBuilder extends SingleThreadedCubeBuilder {
  override def create_build_plan(m: MaterializationScheme, showProgress: Boolean): List[(Set[Int], Int, Int)]  = {
    // aren't they sorted by length by construction?
    val ps = m.projections.zipWithIndex.sortBy(_._1.length).reverse.toList
    assert(ps.head._1.length == m.n_bits)

    // the edges (_2, _3) form a tree rooted at the full cube
    var build_plan: List[(Set[Int], Int, Int)] =
      List((ps.head._1.toSet, ps.head._2, -1))
    val trie = new SetTrieBuildPlan()
    trie.insert(ps.head._1, ps.head._1.length, ps.head._2)

    val pi = new ProgressIndicator(ps.tail.length, "Create Build Plan", showProgress)

    ps.tail.foreach {
      case ((l: List[Int]), (i: Int)) => {
        val s = l.toSet
        val j = trie.getCheapestSuperset(l)
        build_plan = (s, i, j) :: build_plan
        trie.insert(l, l.length, i)
        pi.step
      }
    }
    println
    build_plan.reverse
  }
}
