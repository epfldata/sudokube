//package ch.epfl.data.sudokube
package backend
import util._


/** proxy for C implementation; provides access to native functions
    via JNI.
*/
class CBackend extends Backend[Payload] {
  protected type DENSE_T  = Int // index in C registry data structure
  protected type SPARSE_T = Int

  @native protected def   sRehash0(s_id: Int,
                                   mask: Array[Int]): Int
  @native protected def d2sRehash0(n_bits: Int, d_id: Int,
                                   mask: Array[Int]): Int
  @native protected def s2dRehash0(s_id: Int, d_bits: Int,
                                   mask: Array[Int]): Int
  @native protected def   dRehash0(n_bits: Int, d_id: Int, d_bits: Int,
                                   mask: Array[Int]): Int
  @native protected def mk0(n_bits: Int): Int
  @native protected def sSize0(id: Int): Int
  @native protected def dFetch0(d_id: Int): Array[Int]

  @native protected def add(s_id: Int, n_bits: Int, key: Array[Int], v: Int)
  @native protected def freeze(s_id: Int)

  @native protected def  readSCuboid0(filename: String,
                                      n_bits: Int, size: Int) : Int
  @native protected def  readDCuboid0(filename: String,
                                      n_bits: Int, size: Int) : Int
  @native protected def writeSCuboid0(filename: String, s_id: Int)
  @native protected def writeDCuboid0(filename: String, d_id: Int)

  def readCuboid(id: Int, sparse: Boolean, n_bits: Int, size: BigInt
  ) : Cuboid = {
    val filename = "cub_" + id + ".csuk"
    if(sparse) SparseCuboid(n_bits, readSCuboid0(filename, n_bits, size.toInt))
    else        DenseCuboid(n_bits, readDCuboid0(filename, n_bits, size.toInt))
  }
  def writeCuboid(id: Int, c: Cuboid) {
    val filename = "cub_" + id + ".csuk"
    println("CBackend::writeCuboid: Writing cuboid as " + filename)

    if(c.isInstanceOf[SparseCuboid])
         writeSCuboid0(filename, c.asInstanceOf[SparseCuboid].data)
    else writeDCuboid0(filename, c.asInstanceOf[DenseCuboid].data)
  }

  def mk(n_bits: Int, it: Iterator[(BigBinary, Int)]) : SparseCuboid = {
    val data = mk0(n_bits)

    def add_one(x: (BigBinary, Int)) = {
      val ia_key = x._1.toCharArray(n_bits).map(_.toInt)
      add(data, n_bits, ia_key, x._2)
    }

    it.foreach(add_one(_))

    freeze(data)
    SparseCuboid(n_bits, data)
  }


  // inherited methods seem to add some invisible args that break JNI,
  // so we have yet another indirection.
  protected def   sRehash( s_id: Int, mask: Array[Int]): Int =
                  sRehash0(s_id,      mask)
  protected def d2sRehash( n_bits: Int, d_id: Int, mask: Array[Int]): Int =
                d2sRehash0(n_bits,      d_id,      mask)
  protected def s2dRehash( s_id: Int, d_bits: Int, mask: Array[Int]): Int =
                s2dRehash0(s_id,      d_bits,      mask)
  protected def   dRehash(n_bits: Int, d_id: Int, d_bits: Int,
                          mask: Array[Int]): Int =
                 dRehash0(n_bits, d_id, d_bits, mask)

  protected def dFetch(data: DENSE_T) : Array[Payload] =
    Payload.decode_fetched(dFetch0(data))

  /** size of spare cuboid, in rows. */
  protected def sSize(data: SPARSE_T) : BigInt = sSize0(data)
}


object CBackend {
  System.loadLibrary("CBackend")
  val b = new CBackend
}

