import core.materialization._
import core.prepare._
import frontend.experiments.Tools
import org.scalatest.{FlatSpec, Matchers}
import planning.ProjectionMetaData
import util.Profiler

class PrepareSpec extends FlatSpec with Matchers {

  def isSameAs(oldpmd: Seq[ProjectionMetaData], newpmd: Seq[ProjectionMetaData]) = {
    assert(oldpmd.length == newpmd.length)
    oldpmd.zip(newpmd).foreach{case (o, n) => assert(o.accessible_bits.sameElements(n.accessible_bits) && o.mask.length == n.mask.length)}
  }
  def RMS(nbits: Int, dmin: Int, logncubs: Int, nq: Int, qs: Int, cheap: Int, maxFetch: Int): Unit = {
    val m = RandomizedMaterializationScheme2(nbits, logncubs, dmin, dmin + logncubs)
    (0 until nq).foreach{ i =>
      val q = Tools.rand_q(nbits, qs)
      val oldpo = Profiler("OldPrepareOnline"){ClassicPreparer.prepareOnline(m, q, cheap, maxFetch)}.map(p => ProjectionMetaData(p.accessible_bits, p.accessible_bits0.toList.sorted, p.mask, p.id)).sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      val newpo = Profiler("NewPrepareOnline"){Preparer.default.prepareOnline(m, q, cheap, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      isSameAs(oldpo, newpo)
      val oldpb = Profiler("OldPrepareBatch"){ClassicPreparer.prepareBatch(m, q, maxFetch)}.map(p => ProjectionMetaData(p.accessible_bits, p.accessible_bits0.toList.sorted, p.mask, p.id)).sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      val newpb = Profiler("NewPrepareBatch"){Preparer.default.prepareBatch(m, q, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      isSameAs(oldpb, newpb)
    }
    println("Time for RMS")
    Profiler.print()
  }

  def RMS_online_correctness(nbits: Int, dmin: Int, logncubs: Int, nq: Int, qs: Int, cheap: Int, maxFetch: Int): Unit = {
    val m = RandomizedMaterializationScheme2(nbits, logncubs, dmin, dmin + logncubs)

    (0 until nq).foreach{ i =>
      val q = Tools.rand_q(nbits, qs)
      print(i + " ")
      val oldpo = Profiler("OldPrepareOnline"){ClassicPreparer.prepareOnline(m, q, cheap, maxFetch)}.map(p => ProjectionMetaData(p.accessible_bits, p.accessible_bits0.toList.sorted, p.mask, p.id)).sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      val newpo = Profiler("NewPrepareOnline"){SetTrieOnlinePrepareNoInt.prepareOnline(m, q, cheap, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      val newpo_int = Profiler("NewPrepareOnlineWithInt"){SetTrieOnlinePrepareWithInt.prepareOnline(m, q, cheap, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      isSameAs(oldpo, newpo)
      isSameAs(newpo, newpo_int)
    }
    println("\nTime for RMS")
    Profiler.print()
  }

  def RMS_batch_correctness(nbits: Int, dmin: Int, logncubs: Int, nq: Int, qs: Int, cheap: Int, maxFetch: Int): Unit = {
    val m = RandomizedMaterializationScheme2(nbits, logncubs, dmin, dmin + logncubs)

    (0 until nq).foreach{ i =>
      val q = Tools.rand_q(nbits, qs)
      print(i + " ")
      val oldpb = Profiler("OldPrepareBatch"){SetTrieBatchPrepare1.prepareBatch(m, q, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      val newpb = Profiler("NewPrepareBatchWithInt"){SetTrieBatchPrepareWithInt.prepareBatch(m, q, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      isSameAs(oldpb, newpb)
    }
    println("\nTime for RMS")
    Profiler.print()
  }

  def RMS_performance(nbits: Int, dmin: Int, logncubs: Int, nq: Int, qs: Int, cheap: Int, maxFetch: Int): Unit = {
    val m = RandomizedMaterializationScheme2(nbits, logncubs, dmin, dmin + logncubs)
    (0 until nq).foreach{ i =>
      val q = Tools.rand_q(nbits, qs)
      print(i + " ")
      val oldpo = Profiler("OldPrepareOnline"){ClassicPreparer.prepareOnline(m, q, cheap, maxFetch)}.map(p => ProjectionMetaData(p.accessible_bits, p.accessible_bits0.toList.sorted, p.mask, p.id)).sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      val onlinepo_int = Profiler("NewPrepareOnlineWithInt"){SetTrieOnlinePrepareWithInt.prepareOnline(m, q, cheap, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      val oldpb = Profiler("OldPrepareBatch"){SetTrieBatchPrepare1.prepareBatch(m, q, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      val newpb = Profiler("NewPrepareBatchWithInt"){SetTrieBatchPrepareWithInt.prepareBatch(m, q, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      isSameAs(oldpo, onlinepo_int)
      isSameAs(oldpb, newpb)
    }
    println("\nTime for RMS")
    Profiler.print()
  }

  "Old and New Prepare " should " match " in RMS(200, 15, 15, 100, 10, 40, 40)
  "Old, online_new and online_new_int Prepare " should " match " in RMS_online_correctness(100, 15, 15, 100, 10, 0, 40)
  "New and Batch New Prepare" should " match " in RMS_batch_correctness(100, 15, 15, 100, 10, 40, 40)
  "Old and online, new and batch" should " match " in RMS_performance(100, 15, 15, 100, 10, 40, 40)

}