package frontend.generators

import backend.CBackend
import core.PartialDataCube
import core.materialization.BudgetedSingleSizeMaterializationStrategy


/**
 * Creates a base cuboid where pow(2, d0) out of the pow(2, d) cells
 * contain a non-zero value of 1.
 * Materializes then for each dimension k in [1, d0] "budget"-many
 * cuboids with dimensionality k.
 *
 * cg.saveBase() only needs to be called the first time the generator is executed for a specific setup.
 *
 * @author Thomas Depian
 */
object BudgetedRandomCubeGenerator {
  def main(args: Array[String]): Unit = {
    implicit val backend = CBackend.default
    val nbits = 100
    val d0 = 16
    val b = 0.85
    val cg = RandomCubeGenerator(nbits, d0)
    cg.saveBase()

    (1 to d0).foreach(k => {
      val m = BudgetedSingleSizeMaterializationStrategy(nbits, d0, k, b)
      val cubename = s"${cg.inputname}_${b}_$k"
      val dc = new PartialDataCube(cubename, cg.baseName)
      println(s"Building DataCube $cubename")
      dc.buildPartial(m)
      println(s"Saving DataCube $cubename")
      dc.save()
    })
  }
}