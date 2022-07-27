package frontend.generators
import backend.CBackend
import core.DataCube
import core.materialization.RandomizedMaterializationStrategy
import core.solver.SolverTools

object Warmup {
  def main(args: Array[String]): Unit = {
    val nbits = 10
    val cg = MicroBench(nbits, 100000, 0.5, 0.25)
    val r_its = cg.generatePartitions()
    val sch = cg.schemaInstance
    sch.initBeforeEncode()
    val name = "Warmup"
    val dc = new DataCube(name)
    val m = new RandomizedMaterializationStrategy(nbits, 6, 5)
    val baseCuboid = CBackend.b.mkParallel(sch.n_bits, r_its)
    dc.build(baseCuboid, m)
    dc.primaryMoments = SolverTools.primaryMoments(dc, false)
    dc.save()
    dc.savePrimaryMoments(name)
  }
}
