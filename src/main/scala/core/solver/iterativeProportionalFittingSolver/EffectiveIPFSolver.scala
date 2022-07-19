package core.solver.iterativeProportionalFittingSolver

import util.Bits

import scala.collection.mutable

/**
 * Effective iterative proportional fitting, based on junction tree (or clique tree).
 * TODO: Confirm about collection data structures
 * @author Zhekai Jiang
 * @param querySize Total number of dimensions queried.
 */
class EffectiveIPFSolver(override val querySize: Int) extends IPFSolver(querySize) {

  /**
   * The graph constructed based on the given data cubes, where
   * each node (denoted by an Int) represents a variable, and
   * each pair of nodes are connected if they are in a same cluster.
   * Will be used to construct the junction tree (clique tree) using variable/node elimination.
   */
  object graphicalModel {
    class Edge(val node1: Int, val node2: Int,
               var clusters: mutable.Set[Cluster] /* clusters associated with the edge */ )

    var nodes: mutable.Set[Int] = mutable.Set[Int]() ++ (0 until querySize)
    var adjacencyList: mutable.Map[Int, mutable.Map[Int, Edge]] = mutable.Map[Int, mutable.Map[Int, Edge]]()
      // Int source node -> ( Int destination node -> Edge Set )

    for (variable <- 0 until querySize) {
      adjacencyList += variable -> mutable.Map[Int, Edge]()
    }

    /**
     * For node elimination.
     */
    object minNeighboursOrdering extends Ordering[Int] {
      def compare(a: Int, b: Int): Int = adjacencyList(a).size compare adjacencyList(b).size
    }

    /**
     * Connect all possible pairs of the given nodes.
     * @param nodes Set of nodes that need to be fully connected.
     * @param cluster If the connection is associated with a particular cluster (data cube),
     *                this information will be added to the edge
     *                (to make iterative updates based on the given marginal distributions).
     */
    def connectNodesCompletely(nodes: Set[Int], cluster: Cluster = null): Unit = {
      for (node1 <- nodes; node2 <- nodes) { // connect nodes if they are in a same cluster
        if (node1 != node2) {
          if (!graphicalModel.adjacencyList(node1).contains(node2)) { // no edge from 1 to 2 yet
            val edge = new graphicalModel.Edge(node1, node2, mutable.Set[Cluster]())
            if (cluster != null) {
              edge.clusters += cluster
            }
            graphicalModel.adjacencyList(node1) += (node2 -> edge)
            graphicalModel.adjacencyList(node2) += (node1 -> edge)
          } else { // already added such an edge previously
            if (cluster != null) {
              // associate the edge with the new cluster
              graphicalModel.adjacencyList(node1)(node2).clusters += cluster
              graphicalModel.adjacencyList(node2)(node1).clusters += cluster
            }
          }
        }
      }
    }

    /**
     * Delete a node from the graph (in elimination), including all edges it has.
     * @param node The node to be deleted.
     */
    def deleteNode(node: Int): Unit = {
      for ((destination, _) <- adjacencyList(node)) {
        adjacencyList(destination) -= node
          // delete all edges from other nodes to the node to be deleted
      }
      adjacencyList -= node // delete all edges from the node to be deleted
      nodes -= node // delete node from set of available nodes
    }
  }

  /**
   * The junction tree constructed.
   */
  object junctionTree {

    /**
     * Clique in the junction tree.
     * @param variables Variables associated with this clique, encoded as bits of 1 in an Int.
     */
    class Clique(val variables: Int) {
      val numVariables: Int = getNumOnesInBinary(variables)
      val N: Int = 1 << numVariables
      var marginalDistribution: Array[Double] = Array.fill(N)(1.0 / N)
      var adjacencyList: mutable.Map[Clique, Separator] = mutable.Map[Clique, Separator]()
    }

    /**
     * Separator in the junction tree.
     * @param clique1 One clique associated with the separator.
     * @param clique2 The other clique associated with the separator.
     */
    class Separator(val clique1: Clique, val clique2: Clique) {
      val variables: Int = clique1.variables & clique2.variables
        // intersection of variables in the two cliques, encoded as bits of 1 in an Int
      val numVariables: Int = getNumOnesInBinary(variables)
      val N: Int = 1 << numVariables
      var marginalDistribution: Array[Double] = Array.fill(N)(1.0 / N)
    }

    var cliques: mutable.Set[Clique] = mutable.Set[Clique]()
    var separators: mutable.Set[Separator] = mutable.Set[Separator]()

    /**
     * Construct cliques based on given data cubes.
     */
    def constructCliques(): Unit = {
      while (graphicalModel.nodes.nonEmpty) {
        val nextNode = graphicalModel.nodes.min(graphicalModel.minNeighboursOrdering)
        val clique = constructNewCliqueFromNode(nextNode)
        cliques += clique
        graphicalModel.deleteNode(nextNode)
        graphicalModel.connectNodesCompletely(Bits.fromInt(clique.variables).toSet - nextNode)
      }
    }

    /**
     * Construct a clique from the given node, by adding all its neighbours to the clique
     * — effectively also triangulating the graph.
     * @param node The node from which the clique is to be constructed.
     * @return The clique constructed.
     */
    def constructNewCliqueFromNode(node: Int): junctionTree.Clique = {
      val variables = Bits.toInt((graphicalModel.adjacencyList(node).keySet + node).toSeq)
      new Clique(variables)
    }

    def deleteNonMaximalCliques(): Unit = {
      for (clique <- cliques) {
        if (cliques.exists(clique2 => clique2 != clique && (clique2.variables & clique.variables) == clique.variables)) {
          // variables in clique is a subset of variables in clique2 => clique is not a maximal clique
          deleteClique(clique)
        }
      }
    }

    /**
     * Construct a complete clique graph before constructing junction tree using maximum spanning tree.
     */
    def connectAllCliquesCompletely(): Unit = {
      for (clique1 <- cliques; clique2 <- cliques) {
        if (clique1 != clique2 && !clique1.adjacencyList.contains(clique2)) {
          val separator = new Separator(clique1, clique2)
          clique1.adjacencyList(clique2) = separator
          clique2.adjacencyList(clique1) = separator
          separators += separator
        }
      }
    }

    /**
     * Delete a clique and all separators associated with it from the junction graph.
     * @param clique The clique to be deleted.
     */
    def deleteClique(clique: Clique): Unit = {
      for ((destination, separator) <- clique.adjacencyList) {
        destination.adjacencyList -= clique
        separators -= separator
      }
      cliques -= clique
    }

    /**
     * TODO: confirm about (1) variable missing in all cubes, and (2) disjoint sets of variables => forest instead of tree
     */
    def constructMaximumSpanningTree(): Unit = {
      val remainingCliques: mutable.Set[Clique] = mutable.Set[Clique]()
      cliques.foreach(remainingCliques += _)

      val addedSeparators: mutable.Set[Separator] = mutable.Set[Separator]()
      val separatorsQueue: mutable.PriorityQueue[Separator] = mutable.PriorityQueue[Separator]()(Ordering.by(_.numVariables))

      while (remainingCliques.nonEmpty) { // could be a forest
        val root: Clique = cliques.head
        remainingCliques -= root
        root.adjacencyList.foreach { case (_, separator) => separatorsQueue += separator }

        while (separatorsQueue.nonEmpty) {
          val nextSeparator: Separator = separatorsQueue.dequeue()
          val nextClique = {
            if (remainingCliques.contains(nextSeparator.clique1)) nextSeparator.clique1
            else if (remainingCliques.contains(nextSeparator.clique2)) nextSeparator.clique2
            else null
          }
          if (nextClique != null) {
            nextClique.adjacencyList.foreach { case (_, separator) => separatorsQueue += separator}
            addedSeparators += nextSeparator
            remainingCliques -= nextClique
          }
          // skip if the maximum-weight separator does not cross the cut
          // => which only happens if both cliques are already added
          // since separators are added only after one clique is added
        }
      }

      for (clique <- cliques) {
        clique.adjacencyList.retain { case (_, separator) => addedSeparators.contains(separator) }
      }
      separators.retain(addedSeparators.contains)

    }
  }


  /**
   * Add a new known marginal distribution as a cluster.
   * Create network graph for elimination (triangulation to generate clique tree).
   * @param marginalVariables Sequence of marginal variables.
   * @param marginalDistribution Marginal distribution as a one-dimensional array (values encoded as bits of 1 in index).
   */
  def add(marginalVariables: Seq[Int], marginalDistribution: Array[Double]): Unit = {
    normalizationFactor = marginalDistribution.sum
    val cluster = Cluster(Bits.toInt(marginalVariables), marginalDistribution.map(_ / normalizationFactor))
    clusters = cluster :: clusters
    graphicalModel.connectNodesCompletely(marginalVariables.toSet, cluster)
  }

  /**
   * Obtain the total distribution
   * @return totalDistribution as a one-dimensional array (values encoded as bits of 1 in index).
   */
  def solve(): Array[Double] = {
    constructJunctionTree()
    // iterative update, getTotalDistribution, getSolution
    solution
  }

  /**
   * Construct the junction tree:
   * - triangulate and construct cliques using node elimination
   * - connect all pairs of cliques to get a complete graph (also delete non-maximal cliques at the same time)
   * - obtain maximum spanning tree as junction tree
   */
  def constructJunctionTree(): Unit = {
    junctionTree.constructCliques()
    junctionTree.deleteNonMaximalCliques()
    junctionTree.connectAllCliquesCompletely()
    junctionTree.constructMaximumSpanningTree()
  }
}
