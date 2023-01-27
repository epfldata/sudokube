package frontend.generators

import backend.CBackend
import core.PartialDataCube
import core.materialization.{BudgetedSingleSizeMaterializationStrategy, BudgetedSingleSizePrefixMaterializationStrategy}
import core.materialization.builder.AlwaysBaseCuboidBuilderMT
import util.Profiler


/**
 * Uses for the base cuboid the NYC dataset.
 * Materializes then for each dimension k in [1, d0] "budget"-many
 * cuboids with dimensionality k.
 *
 * cg.saveBase() only needs to be called the first time the generator is executed.
 *
 * @author Thomas Depian
 */
object BudgetedNYCCubeGenerator {
  def main(args: Array[String]): Unit = {
    implicit val backend = CBackend.default
    //val b = 0.25
    val d0 = 20
    val cg = new SSBSample(d0)
    //println(cg.baseName)
    cg.saveBase()

    Profiler(s"${cg.inputname}-Generator b=") {
      (1 to d0).foreach { k =>
        Profiler(s"${cg.inputname}-Generator-Dim$k") {
          Vector(0.25, 1, 4, 16).foreach { b =>
            val m = BudgetedSingleSizeMaterializationStrategy(cg.schemaInstance.n_bits, d0, k, b)
            //println(k -> m.projections.size)
            val cubename = s"${cg.inputname}_random_${b}_$k"
            val dc = new PartialDataCube(cubename, cg.baseName)
            println(s"Building DataCube $cubename")
            dc.buildPartial(m, cb = AlwaysBaseCuboidBuilderMT)
            println(s"Saving DataCube $cubename")
            dc.save()
          }
        }
      }
      Profiler.print()
    }
  }
}