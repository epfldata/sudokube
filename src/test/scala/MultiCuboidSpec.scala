import frontend.schema.StaticSchema
import org.scalatest._


class MultiCuboidSpec extends FlatSpec with Matchers {

  import frontend._
  import frontend.schema._
  import frontend.generators._
  import frontend.experiments.Tools._
  import backend._
  import core._
  import util._
  import scala.reflect.io.Directory
  import java.io.File


  def removeCuboid(name: String) = {
    val directory = new Directory(new File(s"cubedata/$name"))
    directory.deleteRecursively()
  }

  "MultiCuboid Base save and load " should " work for 100 rows" in {
    baseTest(100, "MultiCubeBaseTest100")
  }

  "MultiCuboid Base save and load " should " work for 100k rows" in {
    baseTest(100000, "MultiCubeBaseTest100k")
  }

  "MultiCuboid Base save and load " should " work for 1M rows" in {
    baseTest(1000000, "MultiCubeBaseTest1M")
  }

  "MultiCuboid Base save and load " should " work for 10M rows" in {
    baseTest(10000000, "MultiCubeBaseTest10M")
  }

  //"MultiCuboid Base save and load " should " work for 100M rows" in {
  //  baseTest(100000000, "MultiCubeBaseTest100M")
  //}
  //
  //"MultiCuboid Base save and load " should " work for 600M rows" in {
  //  baseTest(600000000, "MultiCubeBaseTest600M")
  //}
  //
  //"MultiCuboid Base save and load " should " work for 1B rows" in {
  //  baseTest(1000000000, "MultiCubeBaseTest1B")
  //}

  def baseTest(N: Int, name: String) = {
    val n_bits = 200

    val R = (1 to N).map { i => BigBinary(i) -> i.toLong }.toIterator
    val be: Backend[Payload] = CBackend.b
    val full_cube = be.mk(n_bits, R)
    val m = MaterializationScheme.only_base_cuboid(n_bits)
    val dc1 = new DataCube(m)
    dc1.build(full_cube)

    dc1.save2(name)

    val dc2 = DataCube.load2(name)
    val actual = dc2.naive_eval(List(0))
    // 2 (N/2) * (N/2 + 1)/2
    val evenSum = (N.toDouble / 2) * (N.toDouble / 2 + 1)

    //N(N+1)/2 -
    val oddSum = (N.toDouble * (N + 1).toDouble) / 2 - evenSum
    val expected = List(evenSum, oddSum)
    assert(actual.sameElements(expected), s"Expected : ${expected.mkString(" ")}   Actual = ${actual.mkString(" ")}")
    removeCuboid(name)
  }


  "MultiCuboid save and load " should " work for small data " in {
    val n_bits = 10
    val schema = StaticSchema.mk(n_bits)

    val R = (0 to 15).map(i => BigBinary(i) -> i.toLong).toIterator
    val be: Backend[Payload] = CBackend.b
    val full_cube = be.mk(n_bits, R)
    val m = RandomizedMaterializationScheme(schema.n_bits, 1, 1)
    val dc1 = new DataCube(m)
    dc1.build(full_cube)
    val name = "MultiCuboidTest1"
    dc1.save2(name)

    val dc2 = DataCube.load2(name)

    assert(dc1.cuboids.length == dc2.cuboids.length)
    (0 until dc1.cuboids.length).foreach { i =>
      val cub1 = dc1.cuboids(i)
      val cub2 = dc2.cuboids(i)

      assert(cub1.n_bits == cub2.n_bits)
      if (cub1.isInstanceOf[be.SparseCuboid]) {
        assert(cub2.isInstanceOf[be.SparseCuboid])
        assert(cub1.asInstanceOf[be.SparseCuboid].data != cub2.asInstanceOf[be.SparseCuboid].data) //not the same cuboid in backend
      } else {
        assert(cub2.isInstanceOf[be.DenseCuboid])
        val dcub1 = cub1.asInstanceOf[be.DenseCuboid]
        val dcub2 = cub2.asInstanceOf[be.DenseCuboid]
        assert(dcub1.data != dcub2.data)
        val data1 = dcub1.fetch
        val data2 = dcub2.fetch
        assert(data1.sameElements(data2))

      }
      assert(cub1.size == cub2.size)
    }
    removeCuboid(name)
  }

  def randomTest(n_bits: Int, n_rows: Int, rf: Double, base: Double, name: String) = {

    val schema = StaticSchema.mk(n_bits)

    val R = TupleGenerator(schema, n_rows, Sampling.f1)
    val be: Backend[Payload] = CBackend.b
    val full_cube = be.mk(n_bits, R)
    val m = RandomizedMaterializationScheme(schema.n_bits, rf, base)
    val dc1 = new DataCube(m)
    dc1.build(full_cube)
    dc1.save2(name)

    val dc2 = DataCube.load2(name)

    assert(dc1.cuboids.length == dc2.cuboids.length)
    (0 until dc1.cuboids.length).foreach { i =>
      val cub1 = dc1.cuboids(i)
      val cub2 = dc2.cuboids(i)

      assert(cub1.n_bits == cub2.n_bits)
      if (cub1.isInstanceOf[be.SparseCuboid]) {
        assert(cub2.isInstanceOf[be.SparseCuboid])
        assert(cub1.asInstanceOf[be.SparseCuboid].data != cub2.asInstanceOf[be.SparseCuboid].data) //not the same cuboid in backend
      } else {
        assert(cub2.isInstanceOf[be.DenseCuboid])
        val dcub1 = cub1.asInstanceOf[be.DenseCuboid]
        val dcub2 = cub2.asInstanceOf[be.DenseCuboid]
        assert(dcub1.data != dcub2.data)
        val data1 = dcub1.fetch
        val data2 = dcub2.fetch
        assert(data1.sameElements(data2))
      }
      assert(cub1.size == cub2.size)
    }
    removeCuboid(name)
  }

  "MultiCuboid save and load " should " work for medium data medium cuboids " in {
    randomTest(70, 10000, 0.1, 1.1, "MultiCubeTestMM")
  }

  "MultiCuboid save and load " should " work for medium data large cuboids" in {
    randomTest(70, 10000, 0.1, 1.19, "MultiCubeTestML")
  }

  "MultiCuboid save and load " should " work for large data medium cuboids" in {
    randomTest(70, 1000000, 0.1, 1.1, "MultiCubeTestLM")
  }

  "MultiCuboid save and load " should " work for large data dense cuboids" in {
    randomTest(15, 100000, 1, 100, "MultiCubeTestMD")
  }
}