package core.materialization.builder

import backend.Cuboid
import core.materialization.MaterializationScheme
import util.{Bits, ProgressIndicator, Util}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

trait CubeBuilder {
  def build(base: Cuboid, m: MaterializationScheme, showProgress: Boolean = false): IndexedSeq[Cuboid]
}

object CubeBuilder {
  var default: CubeBuilder = TrieCubeBuilderMT
}
trait SingleThreadedCubeBuilder extends CubeBuilder {
  def create_build_plan(m: MaterializationScheme, showProgress: Boolean = false): Seq[(Set[Int], Int, Int)]

  override def build(full_cube: Cuboid, m: MaterializationScheme, showProgress: Boolean = false): IndexedSeq[Cuboid] = {
    val build_plan = create_build_plan(m, showProgress)

    // puts a ref to the same object into all fields of the array.
    val ab = Util.mkAB[Cuboid](m.projections.length, _ => full_cube)

    val backend = full_cube.backend
    val pi = new ProgressIndicator(build_plan.length, "Projecting...", showProgress)

    build_plan.foreach {
      case (_, id, -1) => ab(id) = full_cube
      case (s, id, parent_id) => {
        val bitpos = Bits.mk_list_bitpos(m.projections(parent_id), s)
        ab(id) = ab(parent_id).rehash(bitpos)

        // completion status updates
        if (showProgress) {
          if (ab(id).isInstanceOf[backend.SparseCuboid]) print(".") else print("#")
          pi.step
        }
      }
    }
    ab
  }
}

trait MultiThreadedCubeBuilder extends CubeBuilder {
  def create_build_plan(m: MaterializationScheme, showProgress: Boolean = false): Seq[(Set[Int], Int, Int)]

  override def build(full_cube: Cuboid, m: MaterializationScheme, showProgress: Boolean = false): IndexedSeq[Cuboid] = {
    val build_plan = create_build_plan(m, showProgress)
    val cuboidFutures = Util.mkAB[Future[Cuboid]](m.projections.length, _ => null)
    val pi = new ProgressIndicator(build_plan.length, "Projecting...", showProgress)
    implicit val ec = ExecutionContext.global
    build_plan.foreach {
      case (_, id, -1) => cuboidFutures(id) = Future {full_cube}
      case (s, id, parent_id) =>
        val bitpos = Bits.mk_list_bitpos(m.projections(parent_id), s)
        cuboidFutures(id) = cuboidFutures(parent_id).map { oldcub =>
          val newcub = oldcub.rehash(bitpos)
            pi.step
          newcub
        }
    }
    Await.result(Future.sequence(cuboidFutures), Duration.Inf)
  }
}