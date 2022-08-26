//package ch.epfl.data.sudokube
package backend

import util._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}


/** proxy for C implementation; provides access to native functions
 *via JNI.
 */
class CBackend extends Backend[Payload] {
  protected type DENSE_T = Int // index in C registry data structure
  protected type SPARSE_T = Int
  protected type HYBRID_T = Int //positive is sparse, negative is dense


  override def isDense(h: Int): Boolean = h < 0
  override def extractDense(h: Int): Int = -h
  override def extractSparse(h: Int): Int = h
  override def sparseToHybrid(s: Int): Int = s
  override def denseToHybrid(d: Int): Int = -d

  @native protected def reset0(): Unit
  @native protected def shhash(s_id: Int, pos: Array[Int]): Int
  @native protected def sRehash0(s_id: Int, pos: Array[Int]): Int
  @native protected def d2sRehash0(d_id: Int, pos: Array[Int]): Int
  @native protected def s2dRehash0(s_id: Int, pos: Array[Int]): Int
  @native protected def dRehash0(d_id: Int, pos: Array[Int]): Int

  @native protected def sRehashSlice0(s_id: Int, pos: Array[Int], mask: Array[Boolean]): Array[Long]
  @native protected def dRehashSlice0(d_id: Int, pos: Array[Int], mask: Array[Boolean]): Array[Long]

  @native protected def saveAsTrie0(cuboids: Array[(Array[Int], Int)], filename: String, maxSize: Long)
  @native protected def loadTrie0(filename: String)
  @native protected def prepareFromTrie0(query: Array[Int]): Array[(Int, Long)]

  @native protected def mkAll0(n_bits: Int, n_rows: Int): Int
  @native protected def mk0(n_bits: Int): Int

  @native protected def sSize0(id: Int): Int
  @native protected def sNumBytes0(id: Int): Long

  @native protected def dFetch0(d_id: Int): Array[Long]

  @native protected def cuboidGC0(id: Int): Unit

  @native protected def add_i(i: Int, s_id: Int, n_bits: Int, key: Array[Byte], v: Long)
  @native protected def add(s_id: Int, n_bits: Int, key: Array[Byte], v: Long)
  @native protected def freezePartial(s_id: Int, n_bits: Int)
  @native protected def freeze(s_id: Int)


  @native protected def readMultiCuboid0(filename: String, isSparseArray: Array[Boolean],
                                         nbitsArray: Array[Int], sizeArray: Array[Int]): Array[Int]

  @native protected def writeMultiCuboid0(filename: String, isSparseArray: Array[Boolean], CIdArray: Array[Int])


  override def saveAsTrie(cuboids: Array[(Array[Int], Int)], filename: String, maxSize: Long): Unit = saveAsTrie0(cuboids, filename, maxSize)
  override def loadTrie(filename: String): Unit = loadTrie0(filename)
  override def prepareFromTrie(query: IndexedSeq[Int]): Seq[(Int, Long)] = prepareFromTrie0(query.sorted.toArray).toSeq

  override def reset: Unit = reset0()

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


  def mkAll(n_bits: Int, kvs: Seq[(BigBinary, Long)]) = {
    val nrows = kvs.size
    val data = mkAll0(n_bits, nrows)

    var count = 0
    def add_one(x: (BigBinary, Long)) = {
      val ia_key = x._1.toByteArray(n_bits)
      add_i(count, data, n_bits, ia_key, x._2)
      count += 1
    }
    kvs.foreach(add_one)
    SparseCuboid(n_bits, data)
  }

  /**
   * Initializes a base cuboid using multiple threads, each working on disjoint parts
   * @param n_bits Number of dimensions
   * @param its Array storing, for each thread, the number of key-value pairs as well as iterator to them
   * @return Base Cuboid
   */
  def mkParallel(n_bits: Int, its: IndexedSeq[(Int, Iterator[(BigBinary, Long)])]): SparseCuboid = {

    val sizes = its.map(_._1)
    val pi = new ProgressIndicator(its.size, "Building Base Cuboid", n_bits > 25)
    val totalSize = sizes.sum
    val offsets = Array.fill(its.size)(0)
    (1 until its.size).foreach { i => offsets(i) = offsets(i - 1) + sizes(i - 1) }
    val data = mkAll0(n_bits, totalSize)


    implicit val ec = ExecutionContext.global
    val futs = its.indices.map(i => Future {
      val offset = offsets(i)
      var count = 0
      its(i)._2.foreach { x =>
        val ia_key = x._1.toByteArray(n_bits)
        add_i(count + offset, data, n_bits, ia_key, x._2)
        count += 1
      }
      pi.step
      //println(" P"+i+s" from $offset to ${offset + count}")
      //collection.immutable.BitSet((offset until offset + count):_*)
    })
    Await.result(Future.sequence(futs), Duration.Inf)
    //assert(ranges.reduce(_ union _).size == totalSize)
    SparseCuboid(n_bits, data)
  }
  def mk(n_bits: Int, it: Iterator[(BigBinary, Long)]): SparseCuboid = {
    val data = mk0(n_bits)

    def add_one(x: (BigBinary, Long)) = {
      val ia_key = x._1.toByteArray(n_bits)
      add(data, n_bits, ia_key, x._2)
    }

    it.foreach(add_one(_))
    freeze(data)
    SparseCuboid(n_bits, data)
  }

  def addPartial(n_bits: Int, it: Iterator[(BigBinary, Long)], sc: SparseCuboid): SparseCuboid = {
    val data = sc.data
    def add_one(x: (BigBinary, Long)) = {
      val ia_key = x._1.toByteArray(n_bits)
      add(data, n_bits, ia_key, x._2)
    }

    it.foreach(add_one(_))
    SparseCuboid(n_bits, data)

  }

  def initPartial(): SparseCuboid = {
    SparseCuboid(0, mk0(0))
  }

  def finalisePartial(sc: SparseCuboid): SparseCuboid = {
    val data = sc.data
    freezePartial(data, sc.n_bits)
    SparseCuboid(sc.n_bits, data)
  }


  // inherited methods seem to add some invisible args that break JNI,
  // so we have yet another indirection.

  protected def hybridRehash(s_id: Int, bitpos: IndexedSeq[Int]): Int = {
    shhash(s_id, bitpos.toArray)
  }

  protected def sRehash(s_id: Int, bitpos: IndexedSeq[Int]): Int = {
    sRehash0(s_id, bitpos.toArray)
  }

  protected def d2sRehash(n_bits: Int, d_id: Int, bitpos: IndexedSeq[Int]): Int = {
    d2sRehash0(d_id, bitpos.toArray)
  }

  protected def s2dRehash(s_id: Int, d_bits: Int, bitpos: IndexedSeq[Int]): Int = {
    s2dRehash0(s_id, bitpos.toArray)
  }

  protected def dRehash(n_bits: Int, d_id: Int, d_bits: Int, bitpos: IndexedSeq[Int]): Int = {
    dRehash0(d_id, bitpos.toArray)
  }

  override protected def sRehashSlice(a: Int, bitpos: BITPOS_T, maskArray: Array[Boolean]): Array[Long] = sRehashSlice0(a, bitpos.toArray, maskArray)
  override protected def dRehashSlice(a: Int, bitpos: BITPOS_T, maskArray: Array[Boolean]): Array[Long] = dRehashSlice0(a, bitpos.toArray, maskArray)
  protected def dFetch(data: DENSE_T): Array[Payload] =
    Payload.decode_fetched(dFetch0(data))

  protected def cuboidGC(id: HYBRID_T) = cuboidGC0(id)

  /** size of spare cuboid, in rows. */
  protected def sSize(data: SPARSE_T): BigInt = sSize0(data)
  protected def sNumBytes(data: SPARSE_T): Long = sNumBytes0(data)
}


object CBackend {
  System.loadLibrary("CBackend")
  System.loadLibrary("RowStoreCBackend")
  val original = new CBackend
  val rowstore = new RowStoreCBackend
  val default = rowstore
}

