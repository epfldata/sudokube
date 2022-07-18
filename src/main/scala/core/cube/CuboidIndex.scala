package core.cube

import core.materialization.MaterializationScheme
import planning.NewProjectionMetaData

import java.io.{File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

abstract class CuboidIndex(val n_bits: Int) extends IndexedSeq[IndexedSeq[Int]] {
  val typeName: String
  def saveToFile(filename: String) = {
    val file = new File("cubedata/" + filename + "/" + filename + ".idx")
    val oos = new ObjectOutputStream(new FileOutputStream(file))
    oos.writeObject(typeName)
    saveToOOS(oos)
  }
  protected def saveToOOS(oos: ObjectOutputStream): Unit
  //def merge(that: CuboidIndex): CuboidIndex

  /** Projection without filtering.
   *
   * We shift bits because otherwise the solver/optimizers can't handle them.
   * This id is the index in projections. Assuming that the same ordering
   * is used for the cuboids, it can be used to look up cuboids in the
   * data cube.
   *
   * This returns as many elements as there are in projections.
   * {{{
   *    assert(qproject(q).length == projections.length)
   * }}}
   */
  //no assumption on order
  def qproject(query: IndexedSeq[Int], max_fetch_dim: Int): Seq[NewProjectionMetaData]
  //maintain same order
  def eliminateRedundant(cubs: Seq[NewProjectionMetaData], cheap_size: Int): Seq[NewProjectionMetaData]
  def prepareNaive(query: IndexedSeq[Int]) = prepare(query, n_bits, n_bits)
  def prepareBatch(query: IndexedSeq[Int], max_fetch_dim: Int = n_bits - 1) = prepare(query, max_fetch_dim, max_fetch_dim)
  def prepareOnline(query: IndexedSeq[Int], cheap_size: Int, max_fetch_dim: Int = n_bits) = prepare(query, cheap_size, max_fetch_dim)
  protected def prepare(query: IndexedSeq[Int], cheap_size: Int, max_fetch_dim: Int) = {
    val cubs = qproject(query, max_fetch_dim).sortBy(-_.queryIntersection)
    val cubs2 = eliminateRedundant(cubs, cheap_size)
    if (cheap_size == max_fetch_dim) cubs2 else cubs2.reverse //biggest to smallest for batch mode, reverse for online mode
  }
}


abstract class CuboidIndexFactory {
  def buildFrom(m: MaterializationScheme): CuboidIndex
  def loadFromOIS(ois: ObjectInputStream): CuboidIndex
}

object CuboidIndexFactory {
  var default: CuboidIndexFactory = OptimizedArrayCuboidIndexFactory
  def loadFromFile(filename: String) = {
    val file = new File("cubedata/" + filename + "/" + filename + ".idx")
    val ois = new ObjectInputStream(new FileInputStream(file))
    val typename = ois.readObject.asInstanceOf[String]
    typename match {
      case "Array" => ArrayCuboidIndexFactory.loadFromOIS(ois)
      case "OptimizedArray" => OptimizedArrayCuboidIndexFactory.loadFromOIS(ois)
      case "SetTrie" => SetTrieCuboidIndexFactory.loadFromOIS(ois)
      case _ => throw new IllegalStateException("Cannot load cuboid index -- Unknown index type " + typename)
    }
  }
}
