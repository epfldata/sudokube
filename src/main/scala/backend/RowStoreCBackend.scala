package backend
import com.github.sbt.jni.nativeLoader
@nativeLoader("RowStoreCBackend0") //name + version
class RowStoreCBackend extends CBackend {
  @native override protected def reset0(): Unit
  @native override protected def shhash(s_id: Int, pos: Array[Int]): Int
  @native override protected def sRehash0(s_id: Int, pos: Array[Int]): Int
  @native override protected def d2sRehash0(d_id: Int, pos: Array[Int]): Int
  @native override protected def s2dRehash0(s_id: Int, pos: Array[Int]): Int
  @native override protected def dRehash0(d_id: Int, pos: Array[Int]): Int

  @native override protected def sRehashSlice0(s_id: Int, pos: Array[Int], mask: Array[Boolean]): Array[Long]
  @native override protected def dRehashSlice0(d_id: Int, pos: Array[Int], mask: Array[Boolean]): Array[Long]

  @native override protected def saveAsTrie0(cuboids: Array[(Array[Int], Int)], filename: String, maxSize: Long)
  @native override protected def loadTrie0(filename: String)
  @native override protected def prepareFromTrie0(query: Array[Int]): Array[(Int, Long)]

  @native override protected def mkAll0(n_bits: Int, n_rows: Int): Int
  @native override protected def mk0(n_bits: Int): Int

  @native override protected def sSize0(id: Int): Int
  @native override protected def sNumBytes0(id: Int): Long

  @native override protected def dFetch0(d_id: Int): Array[Long]

  @native override protected def cuboidGC0(id: Int): Unit

  @native override protected def add_i(i: Int, s_id: Int, n_bits: Int, key: Array[Byte], v: Long)
  @native override protected def add(s_id: Int, n_bits: Int, key: Array[Byte], v: Long)
  @native override protected def freezePartial(s_id: Int, n_bits: Int)
  @native override protected def freeze(s_id: Int)


  @native override protected def readMultiCuboid0(filename: String, isSparseArray: Array[Boolean],
                                                  nbitsArray: Array[Int], sizeArray: Array[Int]): Array[Int]
  @native override protected def writeMultiCuboid0(filename: String, isSparseArray: Array[Boolean], CIdArray: Array[Int])

}