package frontend.generators

import backend.CBackend
import core.PartialDataCube
import core.materialization.SingleSizeMaterializationStrategy
import frontend.Sampling
import frontend.schema.encoders.BinaryCol
import frontend.schema.{LD2, Schema2, StaticSchema2}
import util.BigBinary

case class RandomCubeGenerator(n_bits: Int, d0: Int)(implicit backend: CBackend) extends CubeGenerator(s"Random-$n_bits-$d0") {
  override def generatePartitions(): IndexedSeq[(Int, Iterator[(BigBinary, Long)])] = {
    val numRows = 1 << d0
    val numPartsWith1000 = numRows / 1000
    val numParts = 100 min numPartsWith1000
    val sampling_f = Sampling.f1(_)
    val vg = ConstantValueGenerator(1)
    val tg = PartitionedTupleGenerator(schemaInstance, numRows, numParts, sampling_f, vg)
    tg.data
  }
  override protected def schema(): Schema2 = {
    //StaticNatCol with both max and min 1 stores either 0 or 1
    val dims = (0 until n_bits).map(i => LD2(s"d$i", new BinaryCol)).toVector
    new StaticSchema2(dims)
  }
}

object RandomCubeGenerator {
  def main(args: Array[String]) = {
    implicit val backend = CBackend.default
    val nbits = 100
    val cg = RandomCubeGenerator(nbits, 20)
    cg.saveBase()

    List((10, 10)).foreach { case (d, logN) =>
      val m = SingleSizeMaterializationStrategy(nbits, 10, 10)
      val cubename = s"cg.inputname_${d}_${logN}"
      val dc = new PartialDataCube(cubename, cg.baseName)
      println(s"Building DataCube $cubename")
      dc.buildPartial(m)
      println(s"Saving DataCube $cubename")
      dc.save
    }
  }
}
