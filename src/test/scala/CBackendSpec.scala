import frontend.experiments.Tools
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
      CBackend.b.mkAll(d, l.map(x => (BigBinary(x._1), x._2.toLong)))

    val c = my_mk(2, List((2,1), (0,3), (3,7)))
    val d = c.rehash_to_sparse(Array(1,1)) // keep both dimensions
    val e = d.rehash_to_dense(Array(1,0))  // project down to the 1st dimension
    assert(e.fetch.map(_.sm.toInt).toList == List(4, 7))
  }


  "ScalaBackend simple absolute test" should "work" in {
    def my_mk(d: Int, l: List[(Int, Int)]) =
      ScalaBackend.mk(d, l.map(x => (BigBinary(x._1), x._2.toLong)).toIterator)
    val c = my_mk(2, List((2,1), (0,3), (3,7)))
    val d = c.rehash_to_sparse(Array(1,1)) // keep both dimensions
    val e = d.rehash_to_dense(Array(1,0))  // project down to the 1st dimension
    assert(e.fetch.map(_.sm.toInt).toList == List(4, 7))
  }

  "CBackend first projecting to an intermediate query and then down to a lower-dimensional query in three different ways" should "always yield the same result as directly projecting down to the lower-dimensional query" in {

    def mk_mask(d: Int, l: List[Int]) =
      Bits.mk_list_mask(0 to d - 1, l.toSet).toArray

    val n_bits = 70
    val schema = StaticSchema.mk(n_bits)

    for(it <- 1 to 50) {
      val R   = TupleGenerator(schema, 100, Sampling.f1).toList
      val c   = CBackend.b.mkAll(n_bits, R)
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
        qmask).fetch.toList.map(p => new Payload(p.sm, None))

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
  def randomTest(n_bits: Int, n_rows: Int, rf: Double, base: Double, qmax: Int) = {

    val schema = StaticSchema.mk(n_bits)

    val R = TupleGenerator(schema, n_rows, Sampling.f1).toList
    val be = Vector(CBackend.b, ScalaBackend)
    val full_cube = be.map(_.mkAll(n_bits, R))
    val m = RandomizedMaterializationScheme(schema.n_bits, rf, base)
    val dcs = full_cube.map { fc =>
      val dc = new DataCube(m)
      dc.build(fc)
      dc
    }
    val queries = (3 to qmax).flatMap{ s => (1 to 100).map(i => Tools.rand_q(n_bits, s))}
    queries.foreach( q =>  {
      val result = dcs.map(_.naive_eval(q))
        assert(result(0).sameElements(result(1)), s"Query ${q} \n ${result(0).map(_.toInt).mkString(" ")} \n != \n ${result(1).map(_.toInt).mkString(" ")}")

    })
  }
  "Scala and C Backends" should "produce same results for random queries case 0" in  {
    randomTest(70, 100, 0.1, 1.1, 20)
  }
  /*
  "Scala and C Backends" should "produce same results for random queries case 1" in  {
    randomTest(70, 10000, 0.1, 1.1, 20)
  }
  "Scala and C Backends" should "produce same results for random queries case 2 " in  {
    randomTest(70, 10000, 0.1, 1.19, 20)
  }
  "Scala and C Backends" should "produce same results for random queries case 3" in {
    randomTest(70, 1000000, 0.1, 1.1, 20)
  }
  "Scala and C Backends" should "produce same results for random queries case 4" in {
    randomTest(15, 100000, 1, 100, 14)
  }
*/
}



