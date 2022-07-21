package core.materialization.builder

import backend.Cuboid
import core.materialization.MaterializationScheme
import util.{BitUtils, ProgressIndicator, Util}


import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

/**
 * Builds DataCube. For every projection that needs to be materialized, it finds a superset from which
 * it is to be obtained.
 */
trait CubeBuilder {
  def build(base: Cuboid, m: MaterializationScheme, showProgress: Boolean = false): IndexedSeq[Cuboid]

  /** Create a plan for building each cuboid from the smallest that subsumes
   * it. Using BestSubsumerPlanBuilder for this is too expensive though.
   *
   * The resulting build plan starts with the full cube and ends at the
   * zero-dimensional cube. One can iterate through it from begin to end
   * and dependencies will always be ready.
   *
   * The result is a list of triples (a, b, c), where
   * a is the set of dimensions of the projection,
   * b is the key by which the cuboid is to be indexed (which is also
   * its index in projections), and
   * c is the key of the cuboid from which it is to be built.
   * The full cube will have -1 in field c.
   *
   * Note: the build plan construction is deterministic. Given the
   * same projections (in the same order), it always produces the same
   * result.
   */
  protected def create_build_plan(m: MaterializationScheme, showProgress: Boolean = false): Seq[(Set[Int], Int, Int)]

}

object CubeBuilder {
  var default: CubeBuilder = TrieCubeBuilderMT
}
trait SingleThreadedCubeBuilder extends CubeBuilder {
  override def build(full_cube: Cuboid, m: MaterializationScheme, showProgress: Boolean = false): IndexedSeq[Cuboid] = {
    val build_plan = create_build_plan(m, showProgress)

    // puts a ref to the same object into all fields of the array.
    val ab = Util.mkAB[Cuboid](m.projections.length, _ => full_cube)

    val backend = full_cube.backend
    val pi = new ProgressIndicator(build_plan.length, "Projecting...", showProgress)

    build_plan.foreach {
      case (_, id, -1) => ab(id) = full_cube
      case (s, id, parent_id) => {
        val bitpos = BitUtils.mk_list_bitpos(m.projections(parent_id), s)
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

  override def build(full_cube: Cuboid, m: MaterializationScheme, showProgress: Boolean = false): IndexedSeq[Cuboid] = {
    val build_plan = create_build_plan(m, showProgress)
    val cuboidFutures = Util.mkAB[Future[Cuboid]](m.projections.length, _ => null)
    val pi = new ProgressIndicator(build_plan.length, "Projecting...", showProgress)
    implicit val ec = ExecutionContext.global
    build_plan.foreach {
      case (_, id, -1) => cuboidFutures(id) = Future {full_cube}
      case (s, id, parent_id) =>
        val bitpos = BitUtils.mk_list_bitpos(m.projections(parent_id), s)
        cuboidFutures(id) = cuboidFutures(parent_id).map { oldcub =>
          val newcub = oldcub.rehash(bitpos)
            pi.step
          newcub
        }
    }
    Await.result(Future.sequence(cuboidFutures), Duration.Inf)
  }
}