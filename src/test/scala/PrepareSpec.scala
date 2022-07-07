import core.materialization._
import core.prepare._
import frontend.experiments.Tools
import org.scalatest.{FlatSpec, Matchers}
import planning.ProjectionMetaData
import util.Profiler

class PrepareSpec extends FlatSpec with Matchers {

  def isSameAs(oldpmd: Seq[ProjectionMetaData], newpmd: Seq[ProjectionMetaData]) = {
    val oldpmd2 = oldpmd.map(p => ProjectionMetaData(p.accessible_bits, p.accessible_bits0.toList.sorted, p.mask, p.id)).sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
    val newpmd2 = newpmd.map(p => ProjectionMetaData(p.accessible_bits, p.accessible_bits0.toList.sorted, p.mask, p.id)).sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
    assert(oldpmd2.sameElements(newpmd2))
  }
  def RMS(nbits: Int, dmin: Int, logncubs: Int, nq: Int, qs: Int, cheap: Int, maxFetch: Int): Unit = {
    val m = new RandomizedMaterializationScheme(nbits, logncubs, dmin)
    (0 until nq).foreach{ i =>
      val q = Tools.rand_q(nbits, qs)
      val newpo = Profiler("NewPrepareOnline"){Preparer.default.prepareOnline(m, q, cheap, maxFetch)}
      val oldpo = Profiler("OldPrepareOnline"){ClassicPreparer.prepareOnline(m, q, cheap, maxFetch)}
      isSameAs(oldpo, newpo)
      val oldpb = Profiler("OldPrepareBatch"){ClassicPreparer.prepareBatch(m, q, maxFetch)}
      val newpb = Profiler("NewPrepareBatch"){Preparer.default.prepareBatch(m, q, maxFetch)}
      isSameAs(oldpb, newpb)
    }
    println("Time for RMS")
    Profiler.print()
  }

  def RMS_online_correctness(nbits: Int, dmin: Int, logncubs: Int, nq: Int, qs: Int, cheap: Int, maxFetch: Int): Unit = {
    val m = new RandomizedMaterializationScheme(nbits, logncubs, dmin)

    (0 until nq).foreach{ i =>
      val q = Tools.rand_q(nbits, qs)
      print(i + " ")
      val oldpo = Profiler("OldPrepareOnline"){ClassicPreparer.prepareOnline(m, q, cheap, maxFetch)}
      val newpo = Profiler("NewPrepareOnline"){SetTrieOnlinePrepareNoInt.prepareOnline(m, q, cheap, maxFetch)}
      val newpo_int = Profiler("NewPrepareOnlineWithInt"){SetTrieOnlinePrepareWithInt.prepareOnline(m, q, cheap, maxFetch)}
      isSameAs(oldpo, newpo)
      isSameAs(newpo, newpo_int)
    }
    println("\nTime for RMS")
    Profiler.print()
  }

  def RMS_batch_correctness(nbits: Int, dmin: Int, logncubs: Int, nq: Int, qs: Int, cheap: Int, maxFetch: Int): Unit = {
    val m = new RandomizedMaterializationScheme(nbits, logncubs, dmin)

    (0 until nq).foreach{ i =>
      val q = Tools.rand_q(nbits, qs)
      print(i + " ")
      val oldpb = Profiler("OldPrepareBatch"){SetTrieBatchPrepare1.prepareBatch(m, q, maxFetch)}
      val newpb = Profiler("NewPrepareBatchWithInt"){SetTrieBatchPrepareWithInt.prepareBatch(m, q, maxFetch)}
      isSameAs(oldpb, newpb)
    }
    println("\nTime for RMS")
    Profiler.print()
  }

  def RMS_performance(nbits: Int, dmin: Int, logncubs: Int, nq: Int, qs: Int, cheap: Int, maxFetch: Int): Unit = {
    val m = new RandomizedMaterializationScheme(nbits, logncubs, dmin)
    (0 until nq).foreach{ i =>
      val q = Tools.rand_q(nbits, qs)
      print(i + " ")
      val oldpo = Profiler("OldPrepareOnline"){ClassicPreparer.prepareOnline(m, q, cheap, maxFetch)}
      val onlinepo_int = Profiler("NewPrepareOnlineWithInt"){SetTrieOnlinePrepareWithInt.prepareOnline(m, q, cheap, maxFetch)}
      val oldpb = Profiler("OldPrepareBatch"){SetTrieBatchPrepare1.prepareBatch(m, q, maxFetch)}
      val newpb = Profiler("NewPrepareBatchWithInt"){SetTrieBatchPrepareWithInt.prepareBatch(m, q, maxFetch)}
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