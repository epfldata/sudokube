package backend

import com.github.sbt.jni.nativeLoader

import java.nio.ByteBuffer

//@nativeLoader("ColumnStoreCBackend0")
class ColumnStoreCBackend extends CBackend(".csukcs") {
  @native override protected def reset0(): Unit
  @native override protected def cuboidGC0(id: Int): Unit

  @native override protected def sRehash0(s_id: Int, pos: Array[Int], mode: Int): Int
  @native override protected def dRehash0(d_id: Int, pos: Array[Int]): Int

  @native override protected def dFetch0(d_id: Int): Array[Long]

  @native override protected def sSize0(id: Int): Int
  @native override protected def sNumBytes0(id: Int): Long

  @native override protected def mkAll0(n_bits: Int, n_rows: Int): Int
  @native override protected def add_i(startId: Int, s_id: Int, n_bits: Int, numRecords: Int, records: ByteBuffer): Unit
  @native def freezeMkAll(s_id: Int): Unit

  @native override protected def mk0(n_bits: Int): Int
  @native override protected def add(s_id: Int, n_bits: Int, numRecords: Int, records: ByteBuffer): Unit
  @native override protected def freeze(s_id: Int)

  @native override protected def readMultiCuboid0(filename: String, isSparseArray: Array[Boolean],
                                         nbitsArray: Array[Int], sizeArray: Array[Int]): Array[Int]

  @native override protected def writeMultiCuboid0(filename: String, isSparseArray: Array[Boolean], CIdArray: Array[Int])
}
