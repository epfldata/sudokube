import org.scalatest._


class CBackendSpec extends FlatSpec with Matchers {
  import frontend._
  import frontend.schema._
  import frontend.generators._
  import frontend.experiments.Tools._
  import backend._
  import core._
  import util._

  "CBackend simple absolute test" should "work" in {
    def my_mk(d: Int, l: List[(Int, Int)]) =
      CBackend.b.mk(d, l.map(x => (BigBinary(x._1), x._2)).toIterator)

    val c = my_mk(2, List((2,1), (0,3), (3,7)))
    val d = c.rehash_to_sparse(Array(1,1)) // keep both dimensions
    val e = d.rehash_to_dense(Array(1,0))  // project down to the 1st dimension
    assert(e.fetch.map(_.sm.toInt).toList == List(4, 7))
  }

  "CBackend first projecting to an intermediate query and then down to a lower-dimensional query in three different ways" should "always yield the same result as directly projecting down to the lower-dimensional query" in {

    def mk_mask(d: Int, l: List[Int]) =
      Bits.mk_list_mask[Int](0 to d - 1, l.toSet).toArray

    val n_bits = 70
    val schema = StaticSchema.mk(n_bits)

    for(it <- 1 to 50) {
      val R   = TupleGenerator(schema, 100, Sampling.f1).toList
      val c   = CBackend.b.mk(n_bits, R.toIterator)
      val q1  = Util.rnd_choose(n_bits,    6)
      val q2  = Util.rnd_choose(q1.length, 3)
      val qmask = mk_mask(n_bits, q2.map(q1(_)))

      val r1 =  c.rehash_to_dense( qmask).fetch.toList
      val r2 =  c.rehash_to_sparse(mk_mask(n_bits, q1)
                ).rehash_to_dense( mk_mask(q1.length, q2)).fetch.toList
      val r3 =  c.rehash_to_dense( mk_mask(n_bits, q1)
                ).rehash_to_dense( mk_mask(q1.length, q2)).fetch.toList

      assert(r1 == r2, "FAILURE direct != to_sparse(q1).to_dense(q2)")
      assert(r1 == r3, "FAILURE direct !=  to_dense(q1).to_dense(q2)")

      val r4 = ScalaBackend.mk(n_bits, R.toIterator).rehash_to_dense(
        qmask).fetch.toList

      assert(r1 == r4, "FAILURE C != Scala")


      val m = RandomizedMaterializationScheme(n_bits, 1, 1.05)
      val dc = new JailBrokenDataCube(m, c)
      val r5 = dc.getCuboids.last.rehash_to_dense(qmask)
                 .asInstanceOf[CBackend.b.DenseCuboid].fetch.toList

      assert(r1 == r5, "FAILURE C != DataCube.to_dense")

      val r6 = dc.naive_eval(q2.map(q1(_))).toList
      assert(r1.map(_.sm) == r6, "FAILURE C != DataCube.naive_eval")
    }
  }
}



