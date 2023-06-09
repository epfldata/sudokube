package backend

import util.ProgressIndicator


//TODO: Reusing rowstore extension
class TrieStoreCBackend extends CBackend(".csukcs") {
  @native protected def saveTrie0(filename: String)
  @native protected def loadTrie0(filename: String): Unit
  @native protected def initTrie0(maxSize: Long)
  @native protected def setPrimaryMoments0(ps: Array[Double]): Unit
  @native protected def prepareFromTrie0(query: Array[Int], slice: Array[Int]): Array[Double]
  @native protected def saveCuboid0(dims: Array[Int], hybridId: Int): Boolean

  @native override protected def readMultiCuboid0(filename: String, isSparseArray: Array[Boolean],
                                                  nbitsArray: Array[Int], sizeArray: Array[Int]): Array[Int]

  /**
   * Saves the contents of given cuboids using a Trie that stores its moments
   * Experimental feature only in CBackend
   * @param cuboids Array storing, for each cuboid that is to be stored in the trie, the dimensions in that cuboid
   *                (encoded as Int) as well as identifier to the Cuboid
   * @param filename Name of file
   * @param maxSize Maximum node size in the trie. Once the trie capapcity is reached, no additional moments are stored
   */
  def saveAsTrie(cuboids: Seq[(Seq[Int], Int)], pm: Array[Double], filename: String, maxSize: Long): Unit = {
    initTrie0(maxSize)
    setPrimaryMoments0(pm)
    val pi = new ProgressIndicator(cuboids.size)
    var continue = true
    cuboids.zipWithIndex.foreach { case ((ar, hid), cid) =>
      if (continue) {
        continue = saveCuboid0(ar.toArray, hid)
        if (!continue)
          println(s"Terminating early due to lack of space in trie. Progress=${pi.done}/${cuboids.size}")
        pi.step
      }
    }
    saveTrie0(filename)
  }

  /**
   * Loads Trie representation of Cuboids from a file into memory
   * Experimental feature only in CBackend
   * @param filename Name of the file
   */
  def loadTrie(filename: String): Unit = loadTrie0(filename)

  //slice 0, 1 or -1 (for agg)
  def prepareFromTrie(query: IndexedSeq[Int], slice: IndexedSeq[Int]): Array[Double] = prepareFromTrie0(query.toArray, slice.toArray)
}
