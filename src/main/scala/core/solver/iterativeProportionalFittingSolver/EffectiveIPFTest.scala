package core.solver.iterativeProportionalFittingSolver

import org.junit.Test
import util.Bits

class EffectiveIPFTest {
  @Test
  def testAdd(): Unit = {
    val solver = new EffectiveIPFSolver(6)
    solver.add(Seq(0, 1), Array(0.5, 0.5))
    solver.add(Seq(0, 4), Array(0.5, 0.5))
    solver.add(Seq(1, 2), Array(0.5, 0.5))
    solver.add(Seq(2, 3, 4), Array(1.0/3.0, 1.0/3.0, 1.0/3.0))
    solver.add(Seq(2, 3), Array(0.5, 0.5))
    solver.add(Seq(3, 5), Array(0.5, 0.5))
    assert(solver.graphicalModel.adjacencyList(0).exists { case (destination, edge) => destination == 1 && edge.clusters.size == 1 })
    assert(solver.graphicalModel.adjacencyList(0).exists { case (destination, edge) => destination == 4 && edge.clusters.size == 1 })
    assert(solver.graphicalModel.adjacencyList(1).exists { case (destination, edge) => destination == 0 && edge.clusters.size == 1 })
    assert(solver.graphicalModel.adjacencyList(1).exists { case (destination, edge) => destination == 2 && edge.clusters.size == 1 })
    assert(solver.graphicalModel.adjacencyList(2).exists { case (destination, edge) => destination == 1 && edge.clusters.size == 1 })
    assert(solver.graphicalModel.adjacencyList(2).exists { case (destination, edge) => destination == 3 && edge.clusters.size == 2 })
    assert(solver.graphicalModel.adjacencyList(2).exists { case (destination, edge) => destination == 4 && edge.clusters.size == 1 })
    assert(solver.graphicalModel.adjacencyList(3).exists { case (destination, edge) => destination == 2 && edge.clusters.size == 2 })
    assert(solver.graphicalModel.adjacencyList(3).exists { case (destination, edge) => destination == 4 && edge.clusters.size == 1 })
    assert(solver.graphicalModel.adjacencyList(3).exists { case (destination, edge) => destination == 5 && edge.clusters.size == 1 })
    assert(solver.graphicalModel.adjacencyList(5).exists { case (destination, edge) => destination == 3 && edge.clusters.size == 1 })
  }

  @Test
  def testConstructCliques(): Unit = {
    val solver = new EffectiveIPFSolver(6)
    solver.add(Seq(0, 1), Array(0.5, 0.5))
    solver.add(Seq(0, 4), Array(0.5, 0.5))
    solver.add(Seq(1, 2), Array(0.5, 0.5))
    solver.add(Seq(2, 3, 4), Array(1.0/3.0, 1.0/3.0, 1.0/3.0))
    solver.add(Seq(2, 3), Array(0.5, 0.5))
    solver.add(Seq(3, 5), Array(0.5, 0.5))
    solver.junctionTree.constructCliques()
    println(solver.junctionTree.cliques.map(clique => Bits.fromInt(clique.variables)))
  }

  @Test
  def testRetainMaximalCliques(): Unit = {
    val solver = new EffectiveIPFSolver(6)
    solver.add(Seq(0, 1), Array(0.5, 0.5))
    solver.add(Seq(0, 4), Array(0.5, 0.5))
    solver.add(Seq(1, 2), Array(0.5, 0.5))
    solver.add(Seq(2, 3, 4), Array(1.0/3.0, 1.0/3.0, 1.0/3.0))
    solver.add(Seq(2, 3), Array(0.5, 0.5))
    solver.add(Seq(3, 5), Array(0.5, 0.5))
    solver.junctionTree.constructCliques()
    solver.junctionTree.deleteNonMaximalCliques()
    for (clique1 <- solver.junctionTree.cliques; clique2 <- solver.junctionTree.cliques) {
      if (clique1 != clique2) {
        assert((clique1.variables & clique2.variables) != clique1.variables)
      }
    }
  }

  @Test
  def testConstructCompleteCliqueGraph(): Unit = {
    val solver = new EffectiveIPFSolver(6)
    solver.add(Seq(0, 1), Array(0.5, 0.5))
    solver.add(Seq(0, 4), Array(0.5, 0.5))
    solver.add(Seq(1, 2), Array(0.5, 0.5))
    solver.add(Seq(2, 3, 4), Array(1.0/3.0, 1.0/3.0, 1.0/3.0))
    solver.add(Seq(2, 3), Array(0.5, 0.5))
    solver.add(Seq(3, 5), Array(0.5, 0.5))
    solver.junctionTree.constructCliques()
    solver.junctionTree.deleteNonMaximalCliques()
    solver.junctionTree.connectAllCliquesCompletely()
    for (clique1 <- solver.junctionTree.cliques; clique2 <- solver.junctionTree.cliques) {
      if (clique1 != clique2) {
        assert(clique1.adjacencyList.exists { case (destination, separator) =>
            destination == clique2 && separator.variables == (clique1.variables & clique2.variables)
        })
      }
    }
  }

  @Test
  def testConstructJunctionTree(): Unit = {
    val solver = new EffectiveIPFSolver(6)
    solver.add(Seq(0, 1), Array(0.5, 0.5))
    solver.add(Seq(0, 4), Array(0.5, 0.5))
    solver.add(Seq(1, 2), Array(0.5, 0.5))
    solver.add(Seq(2, 3, 4), Array(1.0/3.0, 1.0/3.0, 1.0/3.0))
    solver.add(Seq(2, 3), Array(0.5, 0.5))
    solver.add(Seq(3, 5), Array(0.5, 0.5))
    solver.constructJunctionTree()
    assert(solver.junctionTree.separators.size == solver.junctionTree.cliques.size - 1)
  }
}
