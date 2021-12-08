//package ch.epfl.data.sudokube
package backend
import util._


/** proxy for C implementation; provides access to native functions
    via JNI.
*/
class CBackend extends Backend[Payload] {
  protected type DENSE_T  = Int // index in C registry data structure
  protected type SPARSE_T = Int
  protected type HYBRID_T = Int


  override def isDense(h: Int): Boolean = h < 0
  override def extractDense(h: Int): Int = -h
  override def extractSparse(h: Int): Int = h

  @native protected def shhash(s_id: Int, mask: Array[Int]): Int
  @native protected def   sRehash0(s_id: Int,
                                   mask: Array[Int]): Int
  @native protected def d2sRehash0(n_bits: Int, d_id: Int,
                                   mask: Array[Int]): Int
  @native protected def s2dRehash0(s_id: Int, d_bits: Int,
                                   mask: Array[Int]): Int
  @native protected def   dRehash0(n_bits: Int, d_id: Int, d_bits: Int,
                                   mask: Array[Int]): Int
  @native protected def mkAll0(n_bits: Int, n_rows: Int): Int
  @native protected def mk0(n_bits: Int): Int
  @native protected def sSize0(id: Int): Int
  @native protected def sNumBytes0(id: Int): Long
  @native protected def dFetch0(d_id: Int): Array[Long]

  @native protected def add_i(i: Int, s_id: Int, n_bits: Int, key: Array[Int], v: Long)
  @native protected def add(s_id: Int, n_bits: Int, key: Array[Int], v: Long)
  @native protected def freeze(s_id: Int)

  @native protected def  readSCuboid0(filename: String,
                                      n_bits: Int, size: Int) : Int
  @native protected def  readDCuboid0(filename: String,
                                      n_bits: Int, size: Int) : Int
  @native protected def writeSCuboid0(filename: String, s_id: Int)
  @native protected def writeDCuboid0(filename: String, d_id: Int)

  @native protected def readMultiCuboid0(filename: String, isSparseArray: Array[Boolean],
                                         nbitsArray: Array[Int], sizeArray: Array[Int]): Array[Int]

  @native protected def writeMultiCuboid0(filename: String, isSparseArray: Array[Boolean], CIdArray: Array[Int])


  override def readMultiCuboid(filename: String, idArray: Array[Int], isSparseArray: Array[Boolean], nbitsArray: Array[Int], sizeArray: Array[Int]): Map[Int, Cuboid] = {
    val backend_id_array = readMultiCuboid0(filename, isSparseArray, nbitsArray, sizeArray)
    (0 until idArray.length).map { i =>
      val cub = isSparseArray(i) match {
        case true => SparseCuboid(nbitsArray(i), backend_id_array(i))
        case false => DenseCuboid(nbitsArray(i), backend_id_array(i))
      }
      idArray(i) -> cub
    }.toMap
  }

  override def writeMultiCuboid(filename: String, cuboidsArray: Array[Cuboid]): Unit = {
    val isSparseArray = cuboidsArray.map(_.isInstanceOf[SparseCuboid])
    val backend_id_array = cuboidsArray.map {
      case SparseCuboid(_, data) => data
      case DenseCuboid(_, data) => data
    }
    writeMultiCuboid0(filename, isSparseArray, backend_id_array)
  }

  def readCuboid(id: Int, sparse: Boolean, n_bits: Int, size: BigInt, name_prefix: String): Cuboid = {
    val filename = s"$name_prefix/cub_" + id + ".csuk"
    //WARNING: data not same as id. Do not use data returned by CBackend as id
    if(sparse) SparseCuboid(n_bits, readSCuboid0(filename, n_bits, size.toInt))
    else        DenseCuboid(n_bits, readDCuboid0(filename, n_bits, size.toInt))
  }
  def writeCuboid(id: Int, c: Cuboid, name_prefix: String) {
    val filename = s"$name_prefix/cub_" + id + ".csuk"
    //println("CBackend::writeCuboid: Writing cuboid as " + filename)
    //WARNING: data not same as id. Do not pass id to CBackend
    if(c.isInstanceOf[SparseCuboid])
         writeSCuboid0(filename, c.asInstanceOf[SparseCuboid].data)
    else writeDCuboid0(filename, c.asInstanceOf[DenseCuboid].data)
  }

  def mkAll(n_bits: Int, values: Seq[(BigBinary, Long)]) = {
    val nrows = values.size
    val data = mkAll0(n_bits, nrows)

    var count = 0
    def add_one(x: (BigBinary, Long)) = {
      val ia_key = x._1.toCharArray(n_bits).map(_.toInt)
      add_i(count, data, n_bits, ia_key, x._2)
      count += 1
    }
    values.foreach(add_one)
    SparseCuboid(n_bits, data)
  }
  def mk(n_bits: Int, it: Iterator[(BigBinary, Long)]) : SparseCuboid = {
    val data = mk0(n_bits)

    def add_one(x: (BigBinary, Long)) = {
      val ia_key = x._1.toCharArray(n_bits).map(_.toInt)
      add(data, n_bits, ia_key, x._2)
    }

    it.foreach(add_one(_))

    freeze(data)
    SparseCuboid(n_bits, data)
  }


  // inherited methods seem to add some invisible args that break JNI,
  // so we have yet another indirection.

  protected def hybridRehash(s_id: Int, mask: Array[Int]): Int = shhash(s_id, mask)

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
  protected def sNumBytes(data: SPARSE_T): Long =  sNumBytes0(data)
}


object CBackend {
  System.loadLibrary("CBackend")
  val b = new CBackend
}

