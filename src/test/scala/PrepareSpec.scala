import core.cube.{ArrayCuboidIndexFactory, OptimizedArrayCuboidIndexFactory, SetTrieCuboidIndexFactory}
import core.materialization._
import frontend.experiments.Tools
import org.scalatest.{FlatSpec, Matchers}
import planning.{NewProjectionMetaData, ProjectionMetaData}
import util.{BitUtils, Profiler}

class PrepareSpec extends FlatSpec with Matchers {


  implicit def toNewProjectionMetaData(os: Seq[ProjectionMetaData]) = os.map { o =>
    val abInt = BitUtils.SetToInt(o.accessible_bits)
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


  def RMS_online_test(nbits: Int, dmin: Int, logncubs: Int, nq: Int, qs: Int, cheap: Int, maxFetch: Int): Unit = {
    val m = new RandomizedMaterializationStrategy(nbits, logncubs, dmin)
    val idx1 = ArrayCuboidIndexFactory.buildFrom(m)
    val idx2 = OptimizedArrayCuboidIndexFactory.buildFrom(m)
    val idx3 = SetTrieCuboidIndexFactory.buildFrom(m)
    Profiler.resetAll()
    (0 until nq).foreach { i =>
      val q = Tools.rand_q(nbits, qs)
      print(i + " ")
      val idx1po = Profiler("ArrayCuboidIndex PrepareOnline") { idx1.prepareOnline(q, cheap, maxFetch) }
      val idx2po = Profiler("OptimizedArrayCuboidIndex PrepareOnline") { idx2.prepareOnline(q, cheap, maxFetch) }
      val idx3po = Profiler("SetTrieCuboidIndex PrepareOnline") { idx3.prepareOnline(q, cheap, maxFetch) }

      isSameAs(idx1po, idx2po)
      isSameAs(idx1po, idx3po)
      val ord = NewProjectionMetaData.ordering
      //Check if smaller ones appear first
      val head = idx1po.head
      val last = idx1po.last
      assert((head.queryIntersectionSize <= last.queryIntersectionSize) && (head.cuboidCost <= last.cuboidCost))
      assert(idx1po.sorted(ord) sameElements idx1po.reverse)
      assert(idx2po.sorted(ord) sameElements idx2po.reverse)
      assert(idx3po.sorted(ord) sameElements idx3po.reverse)
    }
    println("\nTime for Online")
    Profiler.print()
  }

  def RMS_batch_test(nbits: Int, dmin: Int, logncubs: Int, nq: Int, qs: Int, maxFetch: Int): Unit = {
    val m = new RandomizedMaterializationStrategy(nbits, logncubs, dmin)
    val idx1 = ArrayCuboidIndexFactory.buildFrom(m)
    val idx2 = OptimizedArrayCuboidIndexFactory.buildFrom(m)
    val idx3 = SetTrieCuboidIndexFactory.buildFrom(m)

    Profiler.resetAll()
    (0 until nq).foreach { i =>
      val q = Tools.rand_q(nbits, qs).toIndexedSeq
      print(i + " ")
      val idx2pb = Profiler("OptimizedArrayCuboidIndex PrepareBatch") { idx2.prepareBatch(q, maxFetch) }
      val idx1pb = Profiler("ArrayCuboidIndex PrepareBatch") { idx1.prepareBatch(q, maxFetch) }
      val idx3pb = Profiler("SetTrieCuboidIndex PrepareBatch") { idx3.prepareBatch(q, maxFetch) }

      isSameAs(idx2pb, idx1pb)
      isSameAs(idx3pb, idx1pb)

      val ord = NewProjectionMetaData.ordering
      //Check if larger ones appear first
      val head = idx1pb.head
      val last = idx1pb.last
      assert((head.queryIntersectionSize >= last.queryIntersectionSize) && (head.cuboidCost >= last.cuboidCost))
      assert(idx1pb.sorted(ord) sameElements idx1pb)
      assert(idx2pb.sorted(ord) sameElements idx2pb)
      assert(idx3pb.sorted(ord) sameElements idx3pb)

    }
    println(s"\nTime for Batch qs=$qs")
    Profiler.print()
  }


  "All cuboid indexes " should " give the same result for Online Prepare QS=15" in RMS_online_test(100, 15, 15, 100, 15, 0, 40)
  //"All cuboid indexes " should " give the same result for Online Prepare2 " in RMS_online_test(100, 15, 15, 100, 10, 20, 40)
  //"All cuboid indexes " should " give the same result for Online Prepare3 " in RMS_online_test(100, 15, 15, 100, 10, 39, 40)
  //"All cuboid indexes" should " give the same result for Batch Prepare QS=5  " in RMS_batch_test(100, 15, 15, 100, 5, 40)
  //"All cuboid indexes" should " give the same result for Batch Prepare QS=10  " in RMS_batch_test(100, 15, 15, 100, 10, 40)
  "All cuboid indexes" should " give the same result for Batch Prepare QS=15  " in RMS_batch_test(100, 15, 15, 100, 15, 40)
  //"All cuboid indexes" should " give the same result for Batch Prepare QS=20  " in RMS_batch_test(100, 15, 15, 100, 20, 40)

}