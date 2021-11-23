//package ch.epfl.data.sudokube
package backend
import combinatorics.Big
import util.BigBinary


/** this is a proxy. */
abstract class Cuboid {
  type MASK_T = Array[Int]
  val n_bits : Int
  def size: BigInt

  def rehash_to_sparse(mask: MASK_T): Cuboid
  def rehash_to_dense( mask: MASK_T): Cuboid

  /** smart rehash */
  def rehash(mask: MASK_T): Cuboid

  def backend: Backend[_]
}


abstract class Backend[MEASURES_T] {
  protected type SPARSE_T
  protected type DENSE_T
  type MASK_T = Array[Int]
  def allones(n: Int): MASK_T = Array.fill(n)(1)
  protected val be_this = this

  case class SparseCuboid(
    n_bits: Int,
    /* private */ val data: SPARSE_T
  ) extends Cuboid {

    /** size in # rows */
    def size = sSize(data)

    override def rehash(mask: MASK_T): Cuboid = {
      assert(mask.length == n_bits)
      val res_n_bits = mask.sum
      val n0 = (math.log(size.toDouble)/math.log(2)).toInt
      if(n0 >= res_n_bits + 10)
        rehash_to_dense(mask)
      else {
        val size_dense = Big.pow2(res_n_bits)
        val sparse_cuboid = rehash_to_sparse(mask)
        if (size_dense <= sparse_cuboid.size)
          sparse_cuboid.rehash_to_dense(allones(res_n_bits))
        else sparse_cuboid
      }

    }

    def rehash_to_dense(mask: MASK_T) = {
      assert(mask.length == n_bits)
      val res_n_bits = mask.sum
      DenseCuboid(res_n_bits, s2dRehash(data, mask.sum, mask))
    }

    def rehash_to_sparse(mask: MASK_T) = {
      assert(mask.length == n_bits)
      SparseCuboid(mask.filter(_ == 1).length, sRehash(data, mask))
    }

    def backend = be_this
  }

  case class DenseCuboid(
    n_bits: Int,
    /* private */ val data: DENSE_T
  ) extends Cuboid {

    def size = Big.pow2(n_bits)


    /** smart rehash */
    override def rehash(mask: MASK_T): Cuboid = rehash_to_dense(mask)

    def rehash_to_dense(mask: MASK_T) = {
      assert(mask.length == n_bits)
      val res_n_bits = mask.sum
      DenseCuboid(res_n_bits, dRehash(n_bits, data, mask.sum, mask))
    }

    def rehash_to_sparse(mask: MASK_T) = {
      assert(mask.length == n_bits)
      SparseCuboid(mask.filter(_ == 1).length, d2sRehash(n_bits, data, mask))
    }

    /** only in DenseCuboid */
    def fetch: Array[MEASURES_T] = dFetch(data)

    def backend = be_this
  }

  def readMultiCuboid(filename: String, idArray: Array[Int], isSparseArray: Array[Boolean], nbitsArray: Array[Int], sizeArray: Array[Int]): Map[Int, Cuboid]
  def writeMultiCuboid(filename: String, cuboidsArray: Array[Cuboid]): Unit

  def readCuboid(id: Int, sparse: Boolean, n_bits: Int, size: BigInt, name_prefix: String): Cuboid
  def writeCuboid(id: Int, c: Cuboid, name_prefix: String): Unit


  def mk(n_bits: Int, it: Iterator[(BigBinary, Long)]) : SparseCuboid

  protected def dFetch(data: DENSE_T) : Array[MEASURES_T]
  protected def sSize(data: SPARSE_T) : BigInt

  protected def d2sRehash(n_bits: Int, a: DENSE_T,  mask: MASK_T) : SPARSE_T
  protected def s2dRehash(a: SPARSE_T, p_bits: Int, mask: MASK_T) : DENSE_T
  protected def   sRehash(a: SPARSE_T,              mask: MASK_T) : SPARSE_T
  protected def   dRehash(n_bits: Int, a: DENSE_T, p_bits: Int,
                                                    mask: MASK_T) : DENSE_T
}


