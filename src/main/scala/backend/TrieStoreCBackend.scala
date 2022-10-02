package backend

class TrieStoreCBackend extends CBackend(".ctrie") {
  @native protected def saveAsTrie0(cuboids: Array[(Array[Int], Int)], filename: String, maxSize: Long)
  @native protected def loadTrie0(filename: String)
  @native protected def prepareFromTrie0(query: Array[Int]): Array[(Int, Long)]

  /**
   * Saves the contents of given cuboids using a Trie that stores its moments
   * Experimental feature only in CBackend
   * @param cuboids Array storing, for each cuboid that is to be stored in the trie, the dimensions in that cuboid
   *                (encoded as Int) as well as identifier to the Cuboid
   * @param filename Name of file
   * @param maxSize Maximum node size in the trie. Once the trie capapcity is reached, no additional moments are stored
   */
  def saveAsTrie(cuboids: Array[(Array[Int], HYBRID_T)], filename: String, maxSize: Long): Unit = saveAsTrie0(cuboids, filename, maxSize)

  /**
   * Loads Trie representation of Cuboids from a file into memory
   * Experimental feature only in CBackend
   * @param filename Name of the file
   */
  def loadTrie(filename: String): Unit = loadTrie0(filename)
  /**
   * Finds moments relevant to a given query from the trie storing moments of several cuboids
   * @return Map containing the value of the moment for the available projections of the query (normalized and encoded using Int)
   *         TODO: Change to MEASURE_T
   *         @see [[core.solver.SolverTools.preparePrimaryMomentsForQuery]]
   */
  def prepareFromTrie(query: IndexedSeq[Int]): Seq[(Int, Long)]=  prepareFromTrie0(query.sorted.toArray).toSeq

}
