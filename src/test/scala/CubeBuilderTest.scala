import core.materialization.{MaterializationStrategy, RandomizedMaterializationStrategy}
import core.materialization.builder._
import org.scalatest.{FlatSpec, Matchers}
import util.Profiler

class CubeBuilderTest extends FlatSpec with Matchers {
  type Plan = Seq[(Set[Int], Int, Int)]
  def checkSame(m: MaterializationStrategy, relaxed: Boolean = false)(plan1: Plan, plan2: Plan) {
    plan1.zip(plan2).foreach{ case(p1, p2)  =>
      assert(p1._1 equals p2._1)
      assert(p1._2 equals p2._2)
      val child = m.projections(p1._2)
      if(p1._1.size == m.n_bits) {
        assert(p1._3 == -1)
        assert(p2._3 == -1)
      } else {
        val parent1 = m.projections(p1._3)
        val parent2 = m.projections(p2._3)
        assert(child.toSet.subsetOf(parent1.toSet))
        assert(child.toSet.subsetOf(parent2.toSet))
        if(!relaxed) assert(parent1.size == parent2.size)
      }
    }
  }
  def randomTest(nbits: Int, logN: Int, minD: Int): Unit = {
    val m = new RandomizedMaterializationStrategy(nbits, logN, minD)
    Profiler.resetAll()
    val plan1 = Profiler("SimplePlan"){SimpleCubeBuilderST.create_build_plan(m)}
    val plan2 = Profiler("BSTPlan"){BinarySearchCubeBuilderST.create_build_plan(m)}
    val plan3 = Profiler("TriePlan"){TrieCubeBuilderST.create_build_plan(m)}
    val notFromBase = plan1.filter(x => x._3 != m.projections.indices.last)
    println("Cuboids not projected from base = " + notFromBase.size)
    assert(plan1.length == m.projections.length)
    assert(plan2.length == m.projections.length)
    assert(plan3.length == m.projections.length)
    checkSame(m, true)(plan1, plan2)
    checkSame(m, false)(plan1, plan3)
    Profiler.print()



  }

  "Cube builders" should "produce same plan as Simple builder for params 10 10 1" in randomTest(10, 10, 1)
  "Cube builders" should "produce same plan as Simple builder for params 100 15 14" in randomTest(100, 15, 14)
  "Cube builders " should "produce same plan as Simple builder for params 30 10 2" in randomTest(30, 10, 2)
  "Cube builders" should "produce same plan as Simple builder for params 100 15 5" in randomTest(100, 15, 5)
}
