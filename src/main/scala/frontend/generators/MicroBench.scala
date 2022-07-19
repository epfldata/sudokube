package frontend.generators

import core.DataCube
import frontend.schema.encoders.StaticNatCol
import frontend.schema.encoders.StaticNatCol.defaultToInt
import frontend.schema.{LD2, Schema2, StaticSchema2, StructuredDynamicSchema}
import util.BigBinary

import scala.util.Random

case class MicroBench(n_bits: Int, total: Long, stddev: Double, prob: Double) extends CubeGenerator(s"microbench_${n_bits}_${total}_${stddev}_${prob}") {

  //println("\n\n----------------------\n" + inputname)
  override def schema(): Schema2 = {
    val enc = (0 until n_bits).map { i => new LD2(s"D$i", new StaticNatCol(0, 1, defaultToInt)) }.toVector
    val sch = new StaticSchema2(enc)
    assert(sch.n_bits == n_bits)
    sch
  }

  def dc = DataCube.load(inputname + "_all")

  override def generate(): (StructuredDynamicSchema, Seq[(BigBinary, Long)]) = ???

  override def generate2(): IndexedSeq[(Int, Iterator[(BigBinary, Long)])] = {
    val keys = (0 until 1 << n_bits)
    val kv = keys.map(k => BigBinary(k) -> sampleValue(k).toLong)
    Vector((kv.size, kv.iterator))
  }

  def sampleValue(k: Int): Int = {
    val hw = BigBinary(k).hamming_weight
    val mean = math.pow(prob, hw) * math.pow(1 - prob, n_bits - hw)
    val phi = math.sqrt(math.pow(stddev * mean, 2) + math.pow(mean, 2))
    val mu = math.log(mean * mean / phi)
    val sigma = math.sqrt(2 * math.log(phi / mean))
    val gauss = math.exp(Random.nextGaussian() * sigma + mu)
    val res = (total * gauss).toInt
    //println(s"$k -> $total * $gauss ($mean) = $res")
    res
  }
}
