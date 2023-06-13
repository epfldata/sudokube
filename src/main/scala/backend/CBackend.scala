//package ch.epfl.data.sudokube
package backend

import util._
import com.github.sbt.jni.nativeLoader

import java.nio.{ByteBuffer, ByteOrder}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}


/** proxy for C implementation; provides access to native functions
 *via JNI.
 */

abstract class CBackend(ext: String) extends Backend[Payload](ext) {
  protected type DENSE_T = Int // index in C registry data structure
  protected type SPARSE_T = Int
  protected type HYBRID_T = Int //positive is sparse, negative is dense

  //------------- NATIVE METHODS ---------------------
  protected def reset0(): Unit = ???
  protected def cuboidGC0(id: Int): Unit = ???

  protected def sRehash0(s_id: Int, pos: Array[Int], mode: Int): Int = ???
  protected def dRehash0(d_id: Int, pos: Array[Int]): Int = ???

  protected def sRehashSlice0(s_id: Int, pos: Array[Int], mask: Array[Boolean]): Array[Long] = ???
  protected def dRehashSlice0(d_id: Int, pos: Array[Int], mask: Array[Boolean]): Array[Long] = ???

  protected def dFetch0(d_id: Int): Array[Long] = ???

  protected def sSize0(id: Int): Int = ???
  protected def sNumBytes0(id: Int): Long = ???

  protected def mkAll0(n_bits: Int, n_rows: Int): Int = ???
  protected def mk0(n_bits: Int): Int = ???
  protected def add_i(startId: Int, s_id: Int, n_bits: Int, numRecords: Int, records: ByteBuffer): Unit = ???
  protected def add(s_id: Int, n_bits: Int, numRecords: Int, records: ByteBuffer): Unit = ???
  protected def freezePartial(s_id: Int, n_bits: Int): Unit = ???
  protected def freeze(s_id: Int): Unit = ???

  protected def readMultiCuboid0(filename: String, isSparseArray: Array[Boolean],
                                 nbitsArray: Array[Int], sizeArray: Array[Int]): Array[Int] = ???

  protected def writeMultiCuboid0(filename: String, isSparseArray: Array[Boolean], CIdArray: Array[Int]): Unit = ???


  //------------------- END NATIVE ----------------------

  override def isDense(h: Int): Boolean = h < 0
  override def extractDense(h: Int): Int = -h
  override def extractSparse(h: Int): Int = h
  override def sparseToHybrid(s: Int): Int = s
  override def denseToHybrid(d: Int): Int = -d

  override def reset: Unit = reset0()
  override def cuboidGC(id: Int): Unit = cuboidGC0(id)
  override def readMultiCuboid(filename: String, idArray: Array[Int], isSparseArray: Array[Boolean], nbitsArray: Array[Int], sizeArray: Array[Int]): Map[Int, Cuboid] = {
    val backend_id_array = readMultiCuboid0(filename, isSparseArray, nbitsArray, sizeArray)
    (0 until idArray.length).map { i =>
      val cub = if (isSparseArray(i)) SparseCuboid(nbitsArray(i), backend_id_array(i))
      else DenseCuboid(nbitsArray(i), backend_id_array(i))
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
    val recSize = (BitUtils.bitToBytes(n_bits)) + 8

    val byteBuffer = java.nio.ByteBuffer.allocateDirect(nrows * recSize)
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN)

    def add_one(x: (BigBinary, Long)) = {
      val ia_key = x._1.toByteArray(n_bits)
      byteBuffer.put(ia_key)
      byteBuffer.putLong(x._2)
    }
    kvs.foreach(add_one)
    add_i(0, data, n_bits, nrows, byteBuffer)
    if(this.isInstanceOf[ColumnStoreCBackend]) this.asInstanceOf[ColumnStoreCBackend].freezeMkAll(data)
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

    val recSize = (BitUtils.bitToBytes(n_bits)) + 8
    implicit val ec = ExecutionContext.global
    val futs = its.indices.map(i => Future {
      val offset = offsets(i)
      val numRecs = its(i)._1
      val byteBuffer = java.nio.ByteBuffer.allocateDirect(numRecs * recSize)
      byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
      its(i)._2.foreach { x =>
        val ia_key = x._1.toByteArray(n_bits)
        byteBuffer.put(ia_key)
        byteBuffer.putLong(x._2)
      }
      add_i(offset, data, n_bits, numRecs, byteBuffer)
      pi.step
      //println(" P"+i+s" from $offset to ${offset + count}")
      //collection.immutable.BitSet((offset until offset + count):_*)
    })
    Await.result(Future.sequence(futs), Duration.Inf)
    //assert(ranges.reduce(_ union _).size == totalSize)

    if(this.isInstanceOf[ColumnStoreCBackend]) this.asInstanceOf[ColumnStoreCBackend].freezeMkAll(data)
    SparseCuboid(n_bits, data)
  }

  /**
   * Initializes base cuboids for multiple measures using multiple threads, each working on disjoint parts
   * @param n_bits Number of dimensions
   * @param its Array storing, for each thread, the number of key-value pairs as well as iterator to them
   * @return Base Cuboids
   */
  def mkParallelMulti(n_bits: Int, numMeasures: Int, its: IndexedSeq[(Int, Iterator[(BigBinary, IndexedSeq[Long])])]): IndexedSeq[SparseCuboid] = {

    val sizes = its.map(_._1)
    val pi = new ProgressIndicator(its.size, "Building Base Cuboids", n_bits > 25)
    val totalSize = sizes.sum
    val offsets = Array.fill(its.size)(0)
    (1 until its.size).foreach { i => offsets(i) = offsets(i - 1) + sizes(i - 1) }
    val multidata = (0 until numMeasures).map{i => mkAll0(n_bits, totalSize)}

    val recSize = (BitUtils.bitToBytes(n_bits)) + 8
    implicit val ec = ExecutionContext.global
    val futs = its.indices.map(i => Future {
      val offset = offsets(i)
      val numRecs = its(i)._1
      val bufferedIterator = its(i)._2.toVector //WARNING: Materializing one entire partition here

      multidata.zipWithIndex.map { case (data, dataidx) =>
        val byteBuffer = java.nio.ByteBuffer.allocateDirect(numRecs * recSize)
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
        bufferedIterator.foreach { x =>
          val ia_key = x._1.toByteArray(n_bits)
          byteBuffer.put(ia_key)
          byteBuffer.putLong(x._2(dataidx))
        }
        add_i(offset, data, n_bits, numRecs, byteBuffer)
      }
      pi.step
      //println(" P"+i+s" from $offset to ${offset + count}")
      //collection.immutable.BitSet((offset until offset + count):_*)
    })
    Await.result(Future.sequence(futs), Duration.Inf)
    //assert(ranges.reduce(_ union _).size == totalSize)

    multidata.map { data =>
      if (this.isInstanceOf[ColumnStoreCBackend]) this.asInstanceOf[ColumnStoreCBackend].freezeMkAll(data)
      SparseCuboid(n_bits, data)
    }
  }
  def mk(n_bits: Int, it: Iterator[(BigBinary, Long)]): SparseCuboid = {
    val data = mk0(n_bits)
    val transferUnits = 64
    val recSize = (BitUtils.bitToBytes(n_bits)) + 8
    val byteBuffer = java.nio.ByteBuffer.allocateDirect(transferUnits * recSize)
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
    def addGroup(xs: Seq[(BigBinary, Long)]) = {
      byteBuffer.clear()
      xs.foreach { x =>
        val ia_key = x._1.toByteArray(n_bits)
        byteBuffer.put(ia_key)
        byteBuffer.putLong(x._2)
      }
      add(data, n_bits, xs.length, byteBuffer)
    }

    it.grouped(transferUnits).foreach(addGroup)
    freeze(data)
    SparseCuboid(n_bits, data)
  }

  def addPartial(n_bits: Int, it: Iterator[(BigBinary, Long)], sc: SparseCuboid): SparseCuboid = {
    val data = sc.data
    val transferUnits = 64
    val recSize = (BitUtils.bitToBytes(n_bits)) + 8
    val byteBuffer = java.nio.ByteBuffer.allocateDirect(transferUnits * recSize)
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
    def addGroup(xs: Seq[(BigBinary, Long)]) = {
      byteBuffer.clear()
      xs.foreach { x =>
        val ia_key = x._1.toByteArray(n_bits)
        byteBuffer.put(ia_key)
        byteBuffer.putLong(x._2)
      }
      add(data, n_bits, transferUnits, byteBuffer)
    }

    it.grouped(transferUnits).foreach(addGroup)
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
    sRehash0(s_id, bitpos.toArray, 3)
  }

  protected def sRehash(s_id: Int, bitpos: IndexedSeq[Int]): Int = {
    sRehash0(s_id, bitpos.toArray, 2)
  }

  protected def d2sRehash(n_bits: Int, d_id: Int, bitpos: IndexedSeq[Int]): Int = ???

  protected def s2dRehash(s_id: Int, d_bits: Int, bitpos: IndexedSeq[Int]): Int = {
    sRehash0(s_id, bitpos.toArray, 1)
  }

  protected def dRehash(n_bits: Int, d_id: Int, d_bits: Int, bitpos: IndexedSeq[Int]): Int = {
    dRehash0(d_id, bitpos.toArray)
  }

  override protected def sRehashSlice(a: Int, bitpos: BITPOS_T, maskArray: Array[Boolean]): Array[Long] = sRehashSlice0(a, bitpos.toArray, maskArray)
  override protected def dRehashSlice(a: Int, bitpos: BITPOS_T, maskArray: Array[Boolean]): Array[Long] = dRehashSlice0(a, bitpos.toArray, maskArray)
  protected def dFetch(data: DENSE_T): Array[Payload] =
    Payload.decode_fetched(dFetch0(data))

  /** size of spare cuboid, in rows. */
  protected def sSize(data: SPARSE_T): BigInt = sSize0(data)
  protected def sNumBytes(data: SPARSE_T): Long = sNumBytes0(data)
}


object CBackend {
  System.loadLibrary("OrigCBackend0")
  System.loadLibrary("RowStoreCBackend0")
  System.loadLibrary("ColumnStoreCBackend0")
  System.loadLibrary("TrieStoreCBackend0")
  val original = new OriginalCBackend
  val rowstore = new RowStoreCBackend
  val colstore = new ColumnStoreCBackend
  val triestore = new TrieStoreCBackend
  val default = colstore
}

