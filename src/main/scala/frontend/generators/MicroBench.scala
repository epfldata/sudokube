package frontend.generators

import backend.CBackend
import core.{DataCube, MaterializationScheme}
import frontend.generators.MB_Sampler.{Exponential, LogNormal, Normal, Sampler, Uniform}
import frontend.schema.encoders.StaticNatCol.defaultToInt
import frontend.schema.encoders.{NatCol, StaticNatCol}
import frontend.schema.{LD2, Schema2, StaticSchema2, StructuredDynamicSchema}
import util.BigBinary

import scala.util.Random

object MB_Sampler extends Enumeration {
  type Sampler = Value
  val Uniform, Normal, LogNormal, Exponential = Value
}
abstract class MicroBench(val n_bits: Int, name: String) extends CubeGenerator(s"mb_$name") {
  override def schema(): Schema2 = {
    val enc = (0 until n_bits).map{i => new LD2(s"D$i", new StaticNatCol(0, 1, defaultToInt))}.toVector
    val sch = new StaticSchema2(enc)
    assert(sch.n_bits == n_bits)
    sch
  }
  def dc = DataCube.load2(inputname+"_all")

  override def generate(): (StructuredDynamicSchema, Seq[(BigBinary, Long)]) = ???

  override def generate2(): (Schema2, IndexedSeq[(Int, Iterator[(BigBinary, Long)])]) =  {
    val sch = schema()
    val keys = (0 until 1 << n_bits)
    val kv = keys.map(k => BigBinary(k) -> sampleValue(k).toLong)
    sch -> Vector((kv.size, kv.iterator))
  }
 def sampleValue(k: Int): Int
}

case class MBSimple(override val n_bits: Int) extends MicroBench(n_bits, s"simple_${n_bits}"){
  override def sampleValue(k: Int): Int = Random.nextInt(100)
}

case class MBValue(maxV: Int) extends MicroBench(10, s"maxv_$maxV"){
  override def sampleValue(k: Int): Int = Random.nextInt(maxV)
}
case class MBSparsity(s: Double) extends MicroBench(12, s"sparsity_$s") {
  override def sampleValue(k: Int): Int = {
    val r = Random.nextDouble()
    if(r < s) 0 else 1+Random.nextInt(99)
  }
}
case class MBProb(p: Double) extends MicroBench(12,s"prob_$p"){
  override def sampleValue(k: Int): Int = {
    val hw = BigBinary(k).hamming_weight
    val p2 = 1.0-math.pow(1-p, hw)  //probability of not having hw 1s
    val r = Random.nextDouble()
    if(r < p2) 0 else Random.nextInt(100)
  }
}

object MicroBenchTest {
  def main(args: Array[String]) = {
    {
      List(6, 8, 10, 12).map(q => MBSimple(q)) ++
        List(10, 100, 1000, 10000).map(mv => MBValue(mv)) ++
          List(0.2, 0.4, 0.6, 0.8).map(s => MBSparsity(s)) ++
        List(0.5, 0.51, 0.52, 0.53).map(p => MBProb(p))
  }.map { cg =>
      val (sch, r_its) = cg.generate2()
      sch.initBeforeEncode()
      val dc = new DataCube(MaterializationScheme.all_cuboids(cg.n_bits))
      dc.build(CBackend.b.mkParallel(sch.n_bits, r_its))
      dc.save2(cg.inputname + "_all")
    }
    ()
  }
}
