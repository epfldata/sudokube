import core.materialization.OldRandomizedMaterializationStrategy
import core.solver.moment.Moment1Transformer
import frontend.experiments.Tools
import org.scalatest._

import java.io.File
import scala.util.Random


class CBackendSpec extends FlatSpec with Matchers {
  import frontend._
  import frontend.schema._
  import frontend.generators._
  import frontend.experiments.Tools._
  import backend._
  import core._
  import util._
  import BitUtils._

  "CBackend simple absolute test" should "work" in {
    def my_mk(d: Int, l: List[(Int, Int)]) =
      CBackend.default.mkAll(d, l.map(x => (BigBinary(x._1), x._2.toLong)))

    val c = my_mk(2, List((2,1), (0,3), (3,7)))
    val d = c.rehash_to_sparse(Array(0, 1)) // keep both dimensions
    val e = d.rehash_to_dense(Array(0))  // project down to the 1st dimension
    assert(e.fetch.map(_.sm.toInt).toList == List(4, 7))
  }

  "CBackend simple absolute test2" should "work" in {

    def my_mk(d: Int, l: Seq[(Int, Int)]) =
      CBackend.default.mkAll(d, l.map(x => (BigBinary(x._1), x._2.toLong)))


    val c = my_mk(100, (1 to 20).map(i => (i, i)))

    assert(c.size == 20)
    assert(c.numBytes == 420)
    def toMask(d : Int, pos: Set[Int]) = (0 until d).filter { i => pos.contains(i)}

    val m0 = Set(0, 1, 2, 3, 4)
    val m1 = m0 ++ Set(20, 21, 22, 23, 24)
    val cs1 = c.rehash(toMask(100, m1))
    assert(cs1.isInstanceOf[CBackend.default.SparseCuboid])

    val cs2 = c.rehash_to_sparse(toMask(100, m1))
    assert(cs2.isInstanceOf[CBackend.default.SparseCuboid])


    assert(cs1.size == 20)
    assert(cs1.size == cs2.size)
    assert(cs1.numBytes == 200)
    assert(cs1.numBytes == cs2.numBytes)


    val cd1 = c.rehash(toMask(100, m0))
    assert(cd1.isInstanceOf[CBackend.default.DenseCuboid])
    assert(cd1.size == 32)
    assert(cd1.numBytes == 256)


    val cd2 = c.rehash_to_dense(toMask(100, m0))
    assert(cd2.isInstanceOf[CBackend.default.DenseCuboid])
    assert(cd2.size == 32)
    assert(cd2.numBytes == 256)


    val cs1d1 = cs1.rehash(toMask(10, m0))
    assert(cs1d1.isInstanceOf[CBackend.default.DenseCuboid])
    assert(cs1d1.size == 32)
    assert(cs1d1.numBytes == 256)


    val cs1d2 = cs1.rehash_to_dense(toMask(10, m0))
    assert(cs1d2.isInstanceOf[CBackend.default.DenseCuboid])
    assert(cs1d2.size == 32)
    assert(cs1d2.numBytes == 256)


    val res = Array.fill(32)(0.0)
    (1 to 20).foreach{ i => res(i) = i.toDouble}

    val results = List(cd1, cd2, cs1d1, cs1d2).map{dcub => dcub.asInstanceOf[CBackend.default.DenseCuboid].fetch.map(_.sm)}
    assert(results.map(_.sameElements(res)).reduce(_ && _))
  }

  "CBackend simple absolute test3" should "work" in {

    def my_mk(d: Int, l: Seq[(Int, Int)]) =
      CBackend.default.mkAll(d, l.map(x => (BigBinary(x._1), x._2.toLong)))


    val c = my_mk(100, (1 to 20).map(i => (i, i)))

    def toMask(d : Int, pos: Set[Int]) = (0 until d).filter(i => pos.contains(i))
    val c1 = c.rehash(toMask(100, Set(0,2,3)))
    assert(c1.isInstanceOf[CBackend.default.DenseCuboid])
    val c2_ = c.rehash(toMask(100, Set(0, 2, 3, 20, 30)))
    val c2 = c2_.rehash(toMask(5, Set(0,1,2)))
    assert(c2.isInstanceOf[CBackend.default.DenseCuboid])

    val c3__ = c.rehash(toMask(100, Set(0,1,2,3,7,10,90)))
    val c3_ = c3__.rehash(toMask(7, Set(0,1,2,3,5)))
    val c3  = c3_.rehash(toMask(5, Set(0, 2, 3)))
    assert(c3.isInstanceOf[CBackend.default.DenseCuboid])
    val res = List(
    0+2+16+18,
    1+3+17+19,
    4+6+20,
    5+7,
    8+10,
    9+11,
    12+14,
    13+15).map(_.toDouble).toArray
    val results = List(c1, c2, c3).map{dcub => dcub.asInstanceOf[CBackend.default.DenseCuboid].fetch.map(_.sm)}
    assert(results.map(_.sameElements(res)).reduce(_ && _))
  }

  "ScalaBackend simple absolute test" should "work" in {
    def my_mk(d: Int, l: List[(Int, Int)]) =
      ScalaBackend.mk(d, l.map(x => (BigBinary(x._1), x._2.toLong)).toIterator)
    val c = my_mk(2, List((2,1), (0,3), (3,7)))
    val d = c.rehash_to_sparse(Array(0, 1)) // keep both dimensions
    val e = d.rehash_to_dense(Array(0))  // project down to the 1st dimension
    assert(e.fetch.map(_.sm.toInt).toList == List(4, 7))
  }

  "CBackend first projecting to an intermediate query and then down to a lower-dimensional query in three different ways" should "always yield the same result as directly projecting down to the lower-dimensional query" in {

    def mk_mask(d: Int, l: IndexedSeq[Int]) =
      mk_list_bitpos(0 to d - 1, l.toSet).toArray

    val n_bits = 70
    val schema = StaticSchema.mk(n_bits)

    for(it <- 1 to 50) {
      val R   = TupleGenerator(schema, 100, Sampling.f1).toList
      val c   = CBackend.default.mkAll(n_bits, R)
      val q1  = Util.rnd_choose(n_bits,    6).toIndexedSeq
      val q2  = Util.rnd_choose(q1.length, 3).toIndexedSeq
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


      val m = OldRandomizedMaterializationStrategy(n_bits, 1, 1.05)
      val dc = new JailBrokenDataCube(m, c)
      val r5 = dc.getCuboids.last.rehash_to_dense(qmask)
                 .asInstanceOf[CBackend.default.DenseCuboid].fetch.toList

      assert(r1 == r5, "FAILURE C != DataCube.to_dense")

      val r6 = dc.naive_eval(q2.map(q1(_))).toList
      assert(r1.map(_.sm) == r6, "FAILURE C != DataCube.naive_eval")
    }
  }
  def randomTest(n_bits: Int, n_rows: Int, rf: Double, base: Double, qmax: Int) = {

    val schema = StaticSchema.mk(n_bits)

    val R = TupleGenerator(schema, n_rows, Sampling.f1).toList
    val be = Vector(CBackend.default, ScalaBackend)
    val full_cube = be.map(_.mkAll(n_bits, R))
    val m = OldRandomizedMaterializationStrategy(schema.n_bits, rf, base)
    val dcs = full_cube.map { fc =>
      val dc = new DataCube()
      dc.build(fc, m)
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

  "CBackend Trie results" should "be correct " ignore {  //TODO: Test ignored for now
    val cubename = "CBackendTrieTest"
    val filename = "cubedata/" + cubename + "/" + cubename + ".ctrie"
    val file = new File(filename)
    if(!file.exists())
      file.getParentFile.mkdirs()

    val nbits = 9
    val N = 1 << nbits
    val data = (0 to 100).map { i =>
      val key = BigBinary(Random.nextInt(512))
      val valueD = math.pow(Random.nextInt(1<<20) + 999999.0, 2)
      val valueLog = math.log(valueD)/math.log(2)
      assert(valueLog < 63)
      val value = valueD.toLong
      key -> value
    }
    val dataArray = Array.fill(N)(0L)

    data.foreach{case (BigBinary(k), v) => dataArray(k.toInt) += v }

    val base = CBackend.default.mk(nbits, data.toIterator)
    val all = 0 until nbits
    val densecub = base.rehash_to_dense(all)
    val densearray = densecub.fetch.map(_.sm )
    assert(densearray.sameElements(dataArray))

    val denseMoments = Moment1Transformer[Double]().getMoments(densearray)

    CBackend.default.saveAsTrie(Array((0 until nbits).toArray -> base.data), filename, N * 2)
    val trieResult  = CBackend.default.prepareFromTrie((0 until nbits))
    val trieMomentArray = Array.fill(N)(0.0)
    trieResult.foreach{case (k, v) => trieMomentArray(k) += v.toDouble }
    val total = dataArray.sum
    trieMomentArray.zip(denseMoments).zipWithIndex.foreach{case ((t,d), i) => if(t.toLong != d.toLong) println(s"$i :: trie ${t.toLong}  actual ${d.toLong}")}
    assert(trieMomentArray.sameElements(denseMoments))

  }
}



