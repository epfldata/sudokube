package core.materialization.builder

import backend.Cuboid
import core.materialization.MaterializationScheme
import util.{Bits, Profiler, ProgressIndicator, Util}

import scala.collection.BitSet

object ParallelCubeBuilder extends CubeBuilder {

  override def build(full_cube: Cuboid, m: MaterializationScheme, showProgress: Boolean): IndexedSeq[Cuboid] = {
    val cores = Runtime.getRuntime.availableProcessors - 2
    val par_build_plan = Profiler("CreateBuildPlan") {
      create_build_plan(cores, m, showProgress)
    }
    // puts a ref to the same object into all fields of the array.
    val ab = Util.mkAB[Cuboid](m.projections.length, _ => full_cube)

    val pi = new ProgressIndicator(par_build_plan.map(_.length).sum, s"Dividing plan into $cores threads", showProgress)
    val full_cube_id = par_build_plan.head.head._2
    ab(full_cube_id) = full_cube

    val threadBuffer = par_build_plan.map { build_plan =>
      new Thread {
        override def run(): Unit = {
          build_plan.foreach {
            case (_, id, -1) => ()
            case (s, id, parent_id) => {
              val mask = Bits.mk_list_mask(m.projections(parent_id), s.toSet).toArray
              ab(id) = ab(parent_id).rehash(mask)

              // completion status updates
              //if(ab(id).isInstanceOf[backend.SparseCuboid]) print(".") else print("#")
              pi.step
            }
          }
        }
      }
    }
    if (showProgress)
      println(s"Starting projections  ")
    threadBuffer.foreach(_.start())
    Profiler("Projections") {
      threadBuffer.foreach(_.join())
    }
    ab
  }

  def create_build_plan(nthreads: Int, m: MaterializationScheme, showProgress: Boolean = false) = {
    val ps = m.projections.zipWithIndex.sortBy(_._1.length).reverse.toList
    assert(ps.head._1.length == m.n_bits)
    import collection.mutable.ArrayBuffer
    // the edges (_2, _3) form a tree rooted at the full cube
    var build_plan = collection.mutable.Map[Int, ArrayBuffer[(BitSet, Int, Int)]]()
    build_plan ++= (0 until nthreads).map { tid => tid -> ArrayBuffer((BitSet(ps.head._1: _*), ps.head._2, -1)) }

    val thread_size = collection.mutable.Map[Int, Int]().withDefaultValue(0)
    val pi = new ProgressIndicator(ps.tail.length, "Create parallel build plan", showProgress)

    ps.tail.foreach {
      case ((l: List[Int]), (i: Int)) => {
        val s = BitSet(l: _*)
        // binary search for good parent. Not always the best parent
        val y = build_plan.mapValues { threadplan =>

          var idxB = threadplan.size - 1
          var idxA = 0
          var mid = (idxA + idxB) / 2
          var cub = threadplan(mid)
          while (idxA <= idxB) {
            mid = (idxA + idxB) / 2
            cub = threadplan(mid)
            if (s.subsetOf(cub._1)) {
              idxA = mid + 1
            } else {
              idxB = mid - 1
            }
          }
          var newiter = mid
          while (s.subsetOf(cub._1) && newiter < threadplan.size - 1) {
            newiter = newiter + 1
            cub = threadplan(newiter)
          }
          while (!s.subsetOf(cub._1)) {
            newiter = newiter - 1
            cub = threadplan(newiter)
          }
          //println(s"\nA=$idxA B=$idxB  mid=$mid  newmid=$newiter i=$i")
          cub
        }
        val y2 = y.tail.foldLeft(y.head) {
          case (acc@(tida, (sa, _, _)), cur@(tidc, (sc, _, _))) =>
            if (sc.size < sa.size) cur
            else if ((sc.size == sa.size) && thread_size(tidc) < thread_size(tida)) cur
            else acc
        }
        val (tid, (s2, j, pj)) = y2
        assert(s.subsetOf(s2))
        build_plan(tid) += ((s, i, j))
        thread_size(tid) += 1
        pi.step
      }
    }
    if (showProgress) {
      println
    }
    val res = build_plan.values.toList
    res
  }
}
