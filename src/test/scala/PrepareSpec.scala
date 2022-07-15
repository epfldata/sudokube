import core.cube.{ArrayCuboidIndexFactory, OptimizedArrayCuboidIndexFactory, SetTrieCuboidIndex, SetTrieCuboidIndexFactory}
import core.materialization._
import core.prepare._
import frontend.experiments.Tools
import org.scalatest.{FlatSpec, Matchers}
import planning.{NewProjectionMetaData, ProjectionMetaData}
import util.{Bits, Profiler}

class PrepareSpec extends FlatSpec with Matchers {


  implicit def toNewProjectionMetaData(os: Seq[ProjectionMetaData]) = os.map { o =>
    val abInt = Bits.toInt(o.accessible_bits)
    val maskpos = o.mask.indices.filter(i => o.mask(i) == 1)
    NewProjectionMetaData(abInt, o.id, o.mask.length, maskpos)
  }

  def isSameAs(x: Seq[NewProjectionMetaData], y: Seq[NewProjectionMetaData]) = {
    assert(x.length == y.length)
    val xy = x zip y
    xy.foreach { case (xp, yp) =>
      assert(xp.queryIntersection == yp.queryIntersection)
      assert(xp.cuboidCost == yp.cuboidCost)
    }
  }
  def RMS(nbits: Int, dmin: Int, logncubs: Int, nq: Int, qs: Int, cheap: Int, maxFetch: Int): Unit = {
    val m = new RandomizedMaterializationScheme(nbits, logncubs, dmin)
    Profiler.resetAll()
    (0 until nq).foreach { i =>
      val q = Tools.rand_q(nbits, qs)
      val newpo = Profiler("NewPrepareOnline") { Preparer.default.prepareOnline(m, q, cheap, maxFetch) }
      val oldpo = Profiler("OldPrepareOnline") { ClassicPreparer.prepareOnline(m, q, cheap, maxFetch) }
      isSameAs(oldpo, newpo)
      val oldpb = Profiler("OldPrepareBatch") { ClassicPreparer.prepareBatch(m, q, maxFetch) }
      val newpb = Profiler("NewPrepareBatch") { Preparer.default.prepareBatch(m, q, maxFetch) }
      isSameAs(oldpb, newpb)
    }
    println("Time for RMS")
    Profiler.print()
  }

  def RMS_online_correctness(nbits: Int, dmin: Int, logncubs: Int, nq: Int, qs: Int, cheap: Int, maxFetch: Int): Unit = {
    val m = new RandomizedMaterializationScheme(nbits, logncubs, dmin)
    val idx1 = ArrayCuboidIndexFactory.buildFrom(m)
    val idx2 = OptimizedArrayCuboidIndexFactory.buildFrom(m)
    val idx3 = SetTrieCuboidIndexFactory.buildFrom(m)
    Profiler.resetAll()
    (0 until nq).foreach { i =>
      val q = Tools.rand_q(nbits, qs).toIndexedSeq
      print(i + " ")
      val oldpo = Profiler("OldPrepareOnline") { ClassicPreparer.prepareOnline(m, q, cheap, maxFetch) }.sortBy(x => Bits.toInt(x.accessible_bits))
      val newpo = Profiler("NewPrepareOnline") { SetTrieOnlinePrepareNoInt.prepareOnline(m, q, cheap, maxFetch) }.sortBy(x => Bits.toInt(x.accessible_bits))
      val newpo_int = Profiler("NewPrepareOnlineWithInt") { SetTrieOnlinePrepareWithInt.prepareOnline(m, q, cheap, maxFetch) }.sortBy(x => Bits.toInt(x.accessible_bits))
      val idx1po = Profiler("Idx1PrepareOnline") { idx1.prepare(q, cheap, maxFetch) }
      val idx2po = Profiler("Idx2PrepareOnline") { idx2.prepare(q, cheap, maxFetch) }
      val idx3po = Profiler("Idx3PrepareOnline") { idx3.prepare(q, cheap, maxFetch) }
      isSameAs(oldpo, newpo)
      isSameAs(newpo, newpo_int)
      isSameAs(oldpo, idx1po)
      isSameAs(idx1po, idx2po)
      isSameAs(idx1po, idx3po)
    }
    println("\nTime for RMS")
    Profiler.print()
  }

  def RMS_batch_correctness(nbits: Int, dmin: Int, logncubs: Int, nq: Int, qs: Int, cheap: Int, maxFetch: Int): Unit = {
    scala.util.Random.setSeed(0)
    val m = new RandomizedMaterializationScheme(nbits, logncubs, dmin)
    val idx1 = ArrayCuboidIndexFactory.buildFrom(m)
    val idx2 = OptimizedArrayCuboidIndexFactory.buildFrom(m)
    val idx3 = SetTrieCuboidIndexFactory.buildFrom(m)

    Profiler.resetAll()
    (0 until nq).foreach { i =>
      val q = Tools.rand_q(nbits, qs).toIndexedSeq
      print(i + " ")
      val oldpb = Profiler("OldPrepareBatch") { SetTrieBatchPrepare1.prepareBatch(m, q, maxFetch)}.sortBy(x => -Bits.toInt(x.accessible_bits))
      val newpb = Profiler("NewPrepareBatchWithInt") { SetTrieBatchPrepareWithInt.prepareBatch(m, q, maxFetch) }.sortBy(x => -Bits.toInt(x.accessible_bits))
      val idx2pb = Profiler("OACI total") { idx2.prepare(q, cheap, maxFetch) }
      val idx1pb = Profiler("ACI total") { idx1.prepare(q, cheap, maxFetch) }
      val idx3pb = Profiler("STCI total") { idx3.prepare(q, cheap, maxFetch) }
      isSameAs(oldpb, newpb)
      isSameAs(oldpb, idx1pb)
      isSameAs(idx2pb, idx1pb)
      isSameAs(idx3pb, idx1pb)
    }
    println("\nTime for RMS")
    Profiler.print()
  }

  def RMS_performance(nbits: Int, dmin: Int, logncubs: Int, nq: Int, qs: Int, cheap: Int, maxFetch: Int): Unit = {
    val m = new RandomizedMaterializationScheme(nbits, logncubs, dmin)
    Profiler.resetAll()
    (0 until nq).foreach { i =>
      val q = Tools.rand_q(nbits, qs)
      print(i + " ")
      val oldpo = Profiler("OldPrepareOnline") { ClassicPreparer.prepareOnline(m, q, cheap, maxFetch) }
      val onlinepo_int = Profiler("NewPrepareOnlineWithInt") { SetTrieOnlinePrepareWithInt.prepareOnline(m, q, cheap, maxFetch) }
      val oldpb = Profiler("OldPrepareBatch") { SetTrieBatchPrepare1.prepareBatch(m, q, maxFetch) }
      val newpb = Profiler("NewPrepareBatchWithInt") { SetTrieBatchPrepareWithInt.prepareBatch(m, q, maxFetch) }
      isSameAs(oldpo, onlinepo_int)
      isSameAs(oldpb, newpb)
    }
    println("\nTime for RMS")
    Profiler.print()
  }

  //"Old and New Prepare " should " match " in RMS(200, 15, 15, 100, 10, 40, 40)
  "Old, online_new and online_new_int Prepare " should " match " in RMS_online_correctness(100, 15, 15, 100, 10, 0, 40)
  "New and Batch New Prepare" should " match " in RMS_batch_correctness(100, 15, 15, 100, 18, 40, 40)
  //"Old and online, new and batch" should " match " in RMS_performance(100, 15, 15, 100, 10, 40, 40)

}