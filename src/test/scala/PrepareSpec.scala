import core.RandomizedMaterializationScheme2
import frontend.experiments.Tools
import org.scalatest.{FlatSpec, Matchers}
import planning.ProjectionMetaData
import util.Profiler

class PrepareSpec extends FlatSpec with Matchers {

  def RMS(nbits: Int, dmin: Int, logncubs: Int, nq: Int, qs: Int, cheap: Int, maxFetch: Int): Unit = {
    val m = RandomizedMaterializationScheme2(nbits, logncubs, dmin + logncubs - 1, 0)
    (0 until nq).foreach{ i =>
      val q = Tools.rand_q(nbits, qs)
      val oldp = Profiler("OldPrepare"){m.prepare_old(q, cheap, maxFetch)}.map(p => ProjectionMetaData(p.accessible_bits, p.accessible_bits0.toList.sorted, p.mask, p.id)).sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      val optp = Profiler("OptPrepare"){m.prepare_opt(q, cheap, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      val newp = Profiler("NewPrepare"){m.prepare_new(q, cheap, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      assert(oldp.filterNot(optp.toSet).isEmpty)
    }
    println("Time for RMS")
    Profiler.print()
  }

  "Old and New Prepare " should " match " in RMS(200, 15, 15, 100, 10, 40, 40)
}
