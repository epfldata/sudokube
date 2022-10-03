package frontend.generators

import backend.CBackend
import core.PartialDataCube
import core.materialization.BudgetedSingleSizeMaterializationStrategy
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
    val d0 = 27
    val b = 0.025
    val cg = NYC()
    // cg.saveBase()


    Profiler("NYC-Generator") {
      (1 to d0).foreach(k => {
        Profiler(s"NYC-Generator-$k") {
          val m = BudgetedSingleSizeMaterializationStrategy(cg.schemaInstance.n_bits, d0, k, b)
          val cubename = s"${cg.inputname}_${b}_$k"
          val dc = new PartialDataCube(cubename, cg.baseName)
          println(s"Building DataCube $cubename")
          dc.buildPartial(m, cb = AlwaysBaseCuboidBuilderMT)
          println(s"Saving DataCube $cubename")
          dc.save()
        }
      })
    }
    Profiler.print()
  }
}