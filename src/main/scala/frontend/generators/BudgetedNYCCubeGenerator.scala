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
    //val b = 0.25

    val cg0 = new SSBSample(20)
    //println(cg.baseName)
    //cg0.saveBase()

    List(
      (new NYC(), 27, -4)
      //(new SSBSample(20), 20, 4),
      //(new RandomCubeGenerator(100, 20), 20, 4)
    ).foreach { case (cg, d0, b0) =>
      Profiler(s"${cg.inputname}-Generator") {
        (2 to 20).foreach { k =>
          Profiler(s"${cg.inputname}-Generator-Dim$k") {
               val b = math.pow(2, b0)
              val m = BudgetedSingleSizeMaterializationStrategy(cg.schemaInstance.n_bits, d0, k, b)
              val cubename = s"${cg.inputname}_random_${b0}_$k" //Changed b to b0
              val dc = new PartialDataCube(cubename, cg.baseName)
              println(s"Building DataCube $cubename")
              dc.buildPartial(m, cb = AlwaysBaseCuboidBuilderMT)
              println(s"Saving DataCube $cubename")
              dc.save()
            }
          backend.reset
          }
        }
        Profiler.print()
      }
    }
}