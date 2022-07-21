import org.scalatest._


class DataCubeSpec extends FlatSpec with Matchers {
  import frontend._
  import frontend.experiments.Tools._
  import backend._
  import core.DataCube
  import util._

//def f(dc: JailBrokenDataCube) {

  /** DataCube::build() creates each cuboid from the cheapest subsuming
      cuboid. Here we create each cuboid directly from the full cuboid
      and compare: must be the same.
  */
  def compare_direct(dc: DataCube) {
    //val fc = dc.getCuboids.last
    val fc = dc.cuboids.last
    val mybackend = fc.backend

    def toLi(c: Cuboid) : List[Int] =
      c.asInstanceOf[mybackend.DenseCuboid].fetch.toList.map(
        _.asInstanceOf[Payload].sm.toInt)

    for(i <- 0 to dc.index.length - 1) {
      val mask = BitUtils.mk_list_bitpos(dc.index.last,
                                        dc.index(i).toSet).toArray

      val c       = dc.cuboids(i) //dc.getCuboids(i)
      val all    = 0 until c.n_bits
      val correct = toLi(fc.rehash_to_dense(mask))
      val readout = toLi( c.rehash_to_dense(all))
      assert(correct == readout)
    }
  }


  "build" should "produce the same cuboids as direct construction for CBackend.b" in {
    compare_direct(mkDC(13, 1, 1.5, 4096,  Sampling.f1, CBackend.b))
    compare_direct(mkDC(16, 1, 1.5, 10000, Sampling.f1, CBackend.b))
  }

  "build" should "produce the same cuboids as direct construction for ScalaBackend" in {
    compare_direct(mkDC(13, 1, 1.5, 4096,  Sampling.f1, ScalaBackend))
  }
}



