package core.solver.iterativeProportionalFittingSolver

import scala.collection.mutable

/**
 * The graphical model constructed based on the given data cubes, where
 * each node (denoted by an Int) represents a variable, and
 * each pair of nodes are connected if they are in a same cluster.
 * @author Zhekai Jiang
 */
object IPFGraphicalModel {
  class Node(val variable: Int) {
    var clusters: mutable.Set[Cluster] = mutable.Set[Cluster]() // For clusters concerning one single variable only
    var adjacencyList: mutable.Map[Node, Edge] = mutable.Map[Node, Edge]() // destination node (variable) -> Edge
  }
  class Edge(val node1: Node, val node2: Node,
                  var clusters: mutable.Set[Cluster] = mutable.Set[Cluster]() /* clusters associated with the edge */ )
}

class IPFGraphicalModel (val numVariables: Int) {

  var nodes: mutable.Map[Int, IPFGraphicalModel.Node] = mutable.Map[Int, IPFGraphicalModel.Node]()
  (0 until numVariables).foreach(n => nodes += (n -> new IPFGraphicalModel.Node(n)))

  /**
   * Connect all possible pairs of the given nodes.
   * @param nodes Set of nodes that need to be fully connected.
   * @param cluster If the connection is associated with a particular cluster (data cube),
   *                this information will be added to the edge
   *                (to make iterative updates based on the given marginal distributions).
   */
  def connectNodesCompletely(nodes: Set[IPFGraphicalModel.Node], cluster: Cluster = null): Unit = {
    if (nodes.size == 1) { // only one variable associated with the cluster, add to the node
      if (cluster != null) {
        nodes.head.clusters += cluster
      }
    } else {
      nodes.foreach(node1 => (nodes - node1).foreach(node2 => {
        if (!node1.adjacencyList.contains(node2)) { // no edge from 1 to 2 yet
          val edge = new IPFGraphicalModel.Edge(node1, node2)
          if (cluster != null) {
            edge.clusters += cluster
          }
          node1.adjacencyList += (node2 -> edge)
          node2.adjacencyList += (node1 -> edge)
        } else { // already added such an edge previously
          if (cluster != null) {
            // associate the edge with the new cluster
            node1.adjacencyList(node2).clusters += cluster
            node2.adjacencyList(node1).clusters += cluster
          }
        }
      }))
    }
  }

  /**
   * Delete a node from the graph (in elimination), including all edges it has.
   * @param node The node to be deleted.
   */
  def deleteNode(node: IPFGraphicalModel.Node): Unit = {
    node.adjacencyList.foreach { case (destination, _) => destination.adjacencyList -= node }
    // delete all edges from other nodes to the node to be deleted
    nodes -= node.variable // delete node from set of available nodes
  }

}
