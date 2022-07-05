package core.materialization.builder

import backend.Cuboid
import core.materialization.MaterializationScheme
import util.{Bits, ProgressIndicator, Util}

trait CubeBuilder {
  def build(base: Cuboid, m: MaterializationScheme, showProgress: Boolean = false): IndexedSeq[Cuboid]
}

trait SingleThreadedCubeBuilder extends CubeBuilder {
  def create_build_plan(m: MaterializationScheme, showProgress: Boolean = false): List[(Set[Int], Int, Int)]

  override def build(full_cube: Cuboid, m: MaterializationScheme, showProgress: Boolean = false): IndexedSeq[Cuboid] = {
    val build_plan = create_build_plan(m, showProgress)

    // puts a ref to the same object into all fields of the array.
    val ab = Util.mkAB[Cuboid](m.projections.length, _ => full_cube)

    val backend = full_cube.backend
    val pi = new ProgressIndicator(build_plan.length, "Projecting...", showProgress)

    build_plan.foreach {
      case (_, id, -1) => ab(id) = full_cube
      case (s, id, parent_id) => {
        val mask = Bits.mk_list_mask(m.projections(parent_id), s).toArray
        ab(id) = ab(parent_id).rehash(mask)

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