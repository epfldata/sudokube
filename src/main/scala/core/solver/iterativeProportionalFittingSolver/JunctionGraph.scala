package core.solver.iterativeProportionalFittingSolver

import core.solver.iterativeProportionalFittingSolver.IPFUtils.isVariablesContained
import util.Bits

import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

/**
 * Definition of junction graphs.
 */
object JunctionGraph {
  /**
   * Clique in the junction tree.
   * @param variables Variables associated with this clique, encoded as bits of 1 in an Int.
   * @param clusters Set of clusters associated with the clique.
   */
  class Clique(var variables: Int, var clusters: mutable.Set[Cluster] = mutable.Set[Cluster]() /* clusters associated with the clique */) {
    var numVariables: Int = IPFUtils.getNumOnesInBinary(variables)
    var N: Int = 1 << numVariables
    var distribution: Array[Double] = Array.fill(N)(1.0 / N) // initialized to uniform
    var adjacencyList: mutable.Map[Clique, Separator] = mutable.Map[Clique, Separator]()
  }

  /**
   * Separator in the junction tree.
   * @param clique1 One clique associated with the separator.
   * @param clique2 The other clique associated with the separator.
   */
  class Separator(val clique1: Clique, val clique2: Clique) {
    var variables: Int = clique1.variables & clique2.variables
    // intersection of variables in the two cliques, encoded as bits of 1 in an Int
    var numVariables: Int = IPFUtils.getNumOnesInBinary(variables)
    var N: Int = 1 << numVariables
    var distribution: Array[Double] = Array.fill(N)(1.0 / N) // initialized to uniform
  }
}

class JunctionGraph {

  var cliques: mutable.Set[JunctionGraph.Clique] = mutable.Set[JunctionGraph.Clique]()
  var separators: mutable.Set[JunctionGraph.Separator] = mutable.Set[JunctionGraph.Separator]()

  /**
   * For use after changing variables in cliques.
   */
  def initializeCliques(): Unit = {
    cliques.foreach(clique => {
      clique.numVariables = IPFUtils.getNumOnesInBinary(clique.variables)
      clique.N = 1 << clique.numVariables
      clique.distribution = Array.fill(clique.N)(1.0 / clique.N)
    })
  }

  /**
   * For use after deleting variables from separator for the loopy scaling update.
   */
  def initializeSeparators(): Unit = {
    separators.foreach(separator => {
      separator.numVariables = IPFUtils.getNumOnesInBinary(separator.variables)
      separator.N = 1 << separator.numVariables
      separator.distribution = Array.fill(separator.N)(1.0 / separator.N)
    })
  }

  /**
   * Construct cliques based on given data cubes.
   */
  def constructCliquesFromGraph(graphicalModel: IPFGraphicalModel): Unit = {
    while (graphicalModel.nodes.nonEmpty) {
      val nextNode = graphicalModel.nodes.values.min(Ordering.by((node: IPFGraphicalModel.Node) => node.adjacencyList.size))
      // min-neighbours — greedy criteria for node elimination ordering
      val clique = constructCliqueFromNode(nextNode)
      cliques += clique
      graphicalModel.connectNodesCompletely(Bits.fromInt(clique.variables).map(graphicalModel.nodes).toSet - nextNode)
      graphicalModel.deleteNode(nextNode)
    }
  }

  /**
   * Construct a clique from the given node, by adding all its neighbours to the clique
   * — effectively also triangulating the graph.
   * @param node The node from which the clique is to be constructed.
   * @return The clique constructed.
   */
  def constructCliqueFromNode(node: IPFGraphicalModel.Node): JunctionGraph.Clique = {
    val variables = Bits.toInt((node.adjacencyList.keySet.map(_.variable) + node.variable).toSeq)
    val clusters =
      node.adjacencyList.map { case (_, edge) => // all clusters in all edges
        edge.clusters.filter(cluster => IPFUtils.isVariablesContained(cluster.variables, variables)) // fully contained in clique
      }.foldLeft(node.clusters.clone())(_ ++ _)
    // If there is a cluster with the node only (1 single marginal variable), add here
    // TODO: add such clusters from other variables in the clique...?
    new JunctionGraph.Clique(variables, clusters)
  }

  /**
   * Delete all non-maximal cliques (whose variables are contained in some other clique).
   * The clusters associated with those cliques will be merged into the their corresponding maximal cliques.
   * TODO: Optimize this?
   */
  def deleteNonMaximalCliques(): Unit = {
    breakable { while (true) {
      (for (clique1 <- cliques; clique2 <- cliques) yield (clique1, clique2)).find { case (containedClique, containingClique) =>
        containedClique != containingClique && isVariablesContained(containedClique.variables, containingClique.variables)
      } match {
        case Some((containedClique, containingClique)) =>
          containingClique.clusters ++= containedClique.clusters
          cliques -= containedClique
        case None => break
      }
    } }
  }

  /**
   * Construct a complete clique graph before constructing junction tree using maximum spanning tree.
   */
  def connectAllCliquesCompletely(): Unit = {
    cliques.foreach(clique1 => (cliques - clique1).foreach(clique2 => {
      if (!clique1.adjacencyList.contains(clique2)) {
        val separator = new JunctionGraph.Separator(clique1, clique2)
        clique1.adjacencyList(clique2) = separator
        clique2.adjacencyList(clique1) = separator
        separators += separator
      }
    }))
  }

  /**
   * Construct the maximum spanning tree, where the weight is based on the number of variables in the separators.
   * Greedily select the maximum-weight separator crossing the cut at each iteration.
   * TODO: confirm about variable missing in all cubes / disjoint sets of variables => forest instead of tree or 0-weight edges ?
   * (adding 0-weight edges seems to work fine)
   */
  def constructMaximumSpanningTree(): Unit = {
    val remainingCliques: mutable.Set[JunctionGraph.Clique] = cliques.clone()

    val addedSeparators: mutable.Set[JunctionGraph.Separator] = mutable.Set[JunctionGraph.Separator]()
    val separatorsQueue: mutable.PriorityQueue[JunctionGraph.Separator] = mutable.PriorityQueue[JunctionGraph.Separator]()(Ordering.by(_.numVariables))

    while (remainingCliques.nonEmpty) { // could be a forest, but actually not if we add 0-weight edges here
      val root: JunctionGraph.Clique = cliques.head
      remainingCliques -= root
      root.adjacencyList.foreach { case (_, separator) => separatorsQueue += separator }

      while (separatorsQueue.nonEmpty) {
        val nextSeparator: JunctionGraph.Separator = separatorsQueue.dequeue()
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

    cliques.foreach(_.adjacencyList.retain { case (_, separator) => addedSeparators.contains(separator) } )
    separators.retain(addedSeparators.contains)

  }

  /**
   * Get a spanning tree with cliques and separators induced by (containing) the given variable, and remove all separators not in this spanning tree.
   * @param variable The variable.
   */
  def retainSpanningTreeForVariable(variable: Int): Unit = {
    val spanningTreeSeparators = constructSpanningTreeWithVariable(variable)
    (separators -- spanningTreeSeparators).foreach(separator => separator.variables &= ~(1 << variable))
  }

  /**
   * Construct a spanning tree from the cliques and separators induced by (containing) the given variable.
   * Here it constructs the maximum spanning tree.
   * @param variable The variable.
   * @return All separators in the spanning tree.
   */
  def constructSpanningTreeWithVariable(variable: Int): mutable.Set[JunctionGraph.Separator] = {
    val remainingCliques: mutable.Set[JunctionGraph.Clique] = cliques.filter(clique => (clique.variables & (1 << variable)) != 0)
    val separators: mutable.Set[JunctionGraph.Separator] = mutable.Set[JunctionGraph.Separator]()
    val separatorsQueue: mutable.PriorityQueue[JunctionGraph.Separator] = mutable.PriorityQueue[JunctionGraph.Separator]()(Ordering.by(_.numVariables))

    val root: JunctionGraph.Clique = remainingCliques.head
    remainingCliques -= root
    root.adjacencyList.foreach {
      case (_, separator) if (separator.variables & (1 << variable)) != 0 => separatorsQueue += separator
      case _ => ()
    }

    while (separatorsQueue.nonEmpty) {
      val nextSeparator: JunctionGraph.Separator = separatorsQueue.dequeue()
      val nextClique = {
        if (remainingCliques.contains(nextSeparator.clique1)) nextSeparator.clique1
        else if (remainingCliques.contains(nextSeparator.clique2)) nextSeparator.clique2
        else null
      }
      if (nextClique != null) {
        nextClique.adjacencyList.foreach {
          case (_, separator) if (separator.variables & (1 << variable)) != 0 => separatorsQueue += separator
          case _ => ()
        }
        separators += nextSeparator
        remainingCliques -= nextClique
      }
      // skip if the maximum-weight separator does not cross the cut
      // => which only happens if both cliques are already added
      // since separators are added only after one clique is added
    }

    separators
  }

  /**
   * Delete separators associated with no variables.
   */
  def deleteZeroSeparators(): Unit = {
    val separatorsToBeDeleted = separators.filter(_.numVariables == 0)
    separatorsToBeDeleted.foreach(separator => {
      separator.clique1.adjacencyList -= separator.clique2
      separator.clique2.adjacencyList -= separator.clique1
      separators -= separator
    })
  }

  /**
   * For testing and demo purposes only.
   */
  def printAllCliquesAndSeparators(): Unit = {
    println(s"\t\t\t${cliques.size} cliques, ${separators.size} separators")
    println("\t\t\tCliques:")
    cliques.foreach(clique =>
      println(s"\t\t\t\t${clique.numVariables} variables: ${Bits.fromInt(clique.variables).mkString(":")}, "
        + s"${clique.clusters.size} clusters, sizes: ${clique.clusters.toList.map(cluster => Bits.fromInt(cluster.variables).size).mkString(":")}, "
        + s"variable occurrences: ${Bits.fromInt(clique.variables).map(variable => clique.clusters.count(cluster => (cluster.variables & (1 << variable)) != 0)).mkString(":")}")
        // "Variable occurrences" mean the number of clusters each variable is covered by, potentially useful for the decomposition-reduction method.
    )
    println("\t\t\tSeparators:")
    separators.foreach(separator =>
      println(s"\t\t\t\t${Bits.fromInt(separator.clique1.variables).mkString(":")} <--> ${Bits.fromInt(separator.clique2.variables).mkString(":")}: ${separator.numVariables} variables: ${Bits.fromInt(separator.variables).mkString(":")}")
    )
  }
}
