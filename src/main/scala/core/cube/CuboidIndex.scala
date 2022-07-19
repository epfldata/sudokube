package core.cube

import core.materialization.MaterializationScheme
import planning.NewProjectionMetaData

import java.io.{File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

/**
 * Index storing what projections are materialized.
 * We use this index to do Prepare, i.e. find which projections are relevant for a given query
 * @param n_bits Number of dimensions of the base cuboid and therefore, the data cube
 */
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
   * This returns as many elements as there are available projections of the query.  There could be several projections
   * of the base cuboid that yield the same query projection after intersection -- this function only returns the cheapeast
   * such cuboid.
   */

  def qproject(query: IndexedSeq[Int], max_fetch_dim: Int): Seq[NewProjectionMetaData]

  /**
   * Eliminates redundant projections from those returned by [[qproject]]. A projection X is redundant if there exists another
   * projection Y such that
   *  -- X is a subset of Y
   *  -- The cost for fetching Y is same as that for X OR the cost for fetching Y is cheap
   */
  def eliminateRedundant(cubs: Seq[NewProjectionMetaData], cheap_size: Int): Seq[NewProjectionMetaData]

  /**
   * Returns the metadata for obtaining the smallest subsuming projection for a given query used by Naive Solver
   */
  def prepareNaive(query: IndexedSeq[Int]) = prepare(query, n_bits, n_bits)

  /**
   * Returns the metadata for obtaining different projections of the query needed by other solvers in batch mode.
   * In batch mode, cheap size is set to maximum so that all sub-projections are marked redundant
   * In batch mode, max_fetch_dim is set to one less than the total so that the base cuboid is never considered for projection
   * In batch mode, the bigger projections appear first followed by smaller ones
   */
  def prepareBatch(query: IndexedSeq[Int], max_fetch_dim: Int = n_bits - 1) = prepare(query, max_fetch_dim, max_fetch_dim)
  /**
   * Returns the metadata for obtaining different projections of the query needed by other solvers in online mode.
   * In online mode, cheap_size is a configurable parameter, usually set to 1 or 2 and  sub-projections are redundant
   * only if they are not cheaper
   * In online mode, max_fetch_dim is set to the total so that the base cuboid is considered for projection
   * In online mode, the smaller projections appear first followed by bigger ones
   */
  def prepareOnline(query: IndexedSeq[Int], cheap_size: Int, max_fetch_dim: Int = n_bits) = prepare(query, cheap_size, max_fetch_dim)
  protected def prepare(query: IndexedSeq[Int], cheap_size: Int, max_fetch_dim: Int) = {
    val cubs = qproject(query, max_fetch_dim).sortBy(-_.queryIntersection) //sorting by supersets first and then subsets
    val cubs2 = eliminateRedundant(cubs, cheap_size)
    if (cheap_size == max_fetch_dim) cubs2 else cubs2.reverse //supersets first then subsets for batch mode, reverse for online mode
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
