import core.cube.{ArrayCuboidIndexFactory, OptimizedArrayCuboidIndexFactory, SetTrieCuboidIndexFactory}
import core.materialization._
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


  def RMS_online_test(nbits: Int, dmin: Int, logncubs: Int, nq: Int, qs: Int, cheap: Int, maxFetch: Int): Unit = {
    val m = new RandomizedMaterializationScheme(nbits, logncubs, dmin)
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

      //Check if subsets before supersets
      assert(idx1po.sortBy(ps => ps.queryIntersection) sameElements idx1po)
      assert(idx2po.sortBy(ps => ps.queryIntersection) sameElements idx2po)
      assert(idx3po.sortBy(ps => ps.queryIntersection) sameElements idx3po)
    }
    println("\nTime for Online")
    Profiler.print()
  }

  def RMS_batch_test(nbits: Int, dmin: Int, logncubs: Int, nq: Int, qs: Int, maxFetch: Int): Unit = {
    val m = new RandomizedMaterializationScheme(nbits, logncubs, dmin)
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

      //Check if supersets before subsets
      assert(idx1pb.sortBy(ps => -ps.queryIntersection) sameElements idx1pb)
      assert(idx2pb.sortBy(ps => -ps.queryIntersection) sameElements idx2pb)
      assert(idx3pb.sortBy(ps => -ps.queryIntersection) sameElements idx3pb)

    }
    println("\nTime for Batch")
    Profiler.print()
  }


  "All cuboid indexes " should " give the same result for Online Prepare " in RMS_online_test(100, 15, 15, 100, 10, 0, 40)
  "All cuboid indexes" should " give the same result for Batch Prepare  " in RMS_batch_test(100, 15, 15, 100, 18, 40)

}