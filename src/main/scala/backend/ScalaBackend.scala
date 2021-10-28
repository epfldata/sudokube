//package ch.epfl.data.sudokube
package backend
import util._
import java.io._


/** a self-contained backend. Here, the data field of the
    DenseCuboid and SparseCuboid instances actually holds the data.
*/
object ScalaBackend extends Backend[Payload] {
  protected type DENSE_T  = Array[Payload]
  protected type SPARSE_T = Seq[(BigBinary, Payload)]


  override def readMultiCuboid(filename: String, idArray: Array[Int], isSparseArray: Array[Boolean],
                               nbitsArray: Array[Int], sizeArray: Array[Int]): Map[Int, Cuboid] = ???

  override def writeMultiCuboid(filename: String, cuboidsArray: Array[Cuboid]): Unit = ???

  def readCuboid(id: Int, sparse: Boolean, n_bits: Int, size: BigInt, name_prefix: String): Cuboid = {
    val ois = new ObjectInputStream(
      new FileInputStream(s"$name_prefix/cub_" + id + ".ssuk"))

    val c = if(sparse) {
      val data = ois.readObject.asInstanceOf[SPARSE_T]
      SparseCuboid(n_bits, data)
    }
    else {
      val data = ois.readObject.asInstanceOf[DENSE_T]
      DenseCuboid(n_bits, data)
    }
    ois.close

    c
  }
  def writeCuboid(id: Int, c: Cuboid, name_prefix: String) {
    val oos = new ObjectOutputStream(
      new FileOutputStream(s"$name_prefix/cub_" + id + ".ssuk"))

    if(c.isInstanceOf[SparseCuboid])
         oos.writeObject(c.asInstanceOf[SparseCuboid].data)
    else oos.writeObject(c.asInstanceOf[DenseCuboid].data)

    oos.close
  }

  def mk(n_bits: Int, it: Iterator[(BigBinary, Long)]) : SparseCuboid = {
    val a : SPARSE_T = it.toList.map(x => (x._1, Payload.mk(x._2)))
    val mask = (1 to n_bits).map(_ => 1).toArray // dummy for deduplication
    SparseCuboid(n_bits, sRehash(a, mask))
  }

  protected def dFetch(data: DENSE_T) : Array[Payload] = data

  protected def sSize(data: SPARSE_T) : BigInt = data.length

  private def d2s(a: DENSE_T) : SPARSE_T =
    a.zipWithIndex.map(x => (BigBinary(x._2), x._1))

  protected def dRehash(n_bits: Int, a: DENSE_T, p_bits: Int, mask: MASK_T
  ) : DENSE_T = {
    s2dRehash(d2s(a), p_bits, mask)
  }

  /** @param n_bits currently not used */
  protected def d2sRehash(n_bits: Int, a: DENSE_T, mask: MASK_T) : SPARSE_T = {
    sRehash(d2s(a), mask)
  }

  protected def s2dRehash(a: SPARSE_T, p_bits: Int, mask: MASK_T) : DENSE_T = {
    val hash_f: BigBinary => Int =
      (x: BigBinary) => Bits.mk_project_f(mask)(x).toInt

    val a2 = Util.mkAB[Payload](1 << p_bits, _ => Payload.none)
    a.foreach { case(i, v) => a2(hash_f(i)).merge_in(v) }
    a2.toArray
  }

  protected def sRehash(a: SPARSE_T, mask: MASK_T) : SPARSE_T = {
    val hash_f: BigBinary => BigBinary = Bits.mk_project_f(mask)

    def dedup(b: SPARSE_T) : SPARSE_T =
       b.groupBy(_._1).mapValues(x => Payload.sum(x.map(_._2))).toList

    dedup(a.map{ case (i, v) => (hash_f(i), v) })
  }
}


