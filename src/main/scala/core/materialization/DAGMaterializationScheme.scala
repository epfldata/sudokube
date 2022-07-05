package core.materialization

import planning.ProjectionMetaData
import util.Bits

import scala.collection.mutable.ListBuffer

case class DAGMaterializationScheme(m: MaterializationScheme) extends MaterializationScheme(m.n_bits) {
  /** the metadata describing each projection in this scheme. */
  override val projections: IndexedSeq[List[Int]] = m.projections
  val proj_zip_sorted = projections.zipWithIndex.sortBy(-_._1.length)

  /**
   * A directed acyclic graph representation of the projections, root is the full dimension projection. Has an edge from A to B if A.contains(B)
   */
  var projectionsDAGroot = buildDag()



  /*def buildDag(): DagVertex = {
    //val DAG = new mutable.HashMap[Int, List[DagVertex]]().withDefaultValue(Nil) //default value for List[DagVertex] to avoid checking if entry already exists

    val root = new DagVertex(proj_zip_sorted.head._1, proj_zip_sorted.head._1.length, proj_zip_sorted.head._2)
    var addedVtcs = 1
    var i = 1
    proj_zip_sorted.tail.foreach { case (p, id) =>
      print("Curr : " + i + "\n")
      i += 1
      val new_dag_v = new DagVertex(p, p.size, id+1)
      var vertexRet = 0
      val queue = collection.mutable.Queue[(DagVertex, Seq[Int])]()
      queue.enqueue((root, root.p))
      while (queue.nonEmpty) {
        val deq_dagV = queue.dequeue()
        val queue_oldsize = queue.size
        deq_dagV._1.children.foreach(child =>
          if (p.forall(p_dim => child._1.p.contains(p_dim))) {
            queue.enqueue((child._1, deq_dagV._1.p))
          }
        )
        if (queue_oldsize == queue.size) {
          deq_dagV._1.addChild(new_dag_v)
          vertexRet += 1
        }
      }

      if (vertexRet == 0) {
        println("Error while adding projection vertex " + (id+1) + " : doesn't have any parent")
      } else {
        addedVtcs += 1
      }
    }
    if (addedVtcs != projections.length) {
      println("Error, not all vertices were added.")
    }
    root
  }*/

  /**
   *
   * @return The root of the DAG
   */
  def buildDag(): DagVertex = {
    //val DAG = new mutable.HashMap[Int, List[DagVertex]]().withDefaultValue(Nil) //default value for List[DagVertex] to avoid checking if entry already exists

    val root = new DagVertex(proj_zip_sorted.head._1, proj_zip_sorted.head._1.length, proj_zip_sorted.head._2)
    var addedVtcs = 1
    var i = 1
    proj_zip_sorted.tail.foreach { case (p, id) =>
      print("Curr : " + i + "\n")
      i += 1
      val new_vert = new DagVertex(p, p.size, id)
      var vertexRet = 0
      val queue = collection.mutable.Queue[DagVertex]()
      queue.enqueue(root)
      while (queue.nonEmpty) {
        val deq_vert = queue.dequeue()
        val queue_oldsize = queue.size
        deq_vert.children.foreach(child => {
          if (p.forall(p_dim => child._1.p.contains(p_dim))) {
            queue.enqueue(child._1)
          }
        }
        )
        if (queue_oldsize == queue.size) {
          deq_vert.addChild(new_vert)
          vertexRet += 1
        }
      }
      if (vertexRet == 0) {
        println("Error while adding projection vertex " + (id + 1) + " : doesn't have any parent")
      } else {
        addedVtcs += 1
      }
    }
    if (addedVtcs != projections.length) {
      println("Error, not all vertices were added.")
    }
    root
  }

  /**
   * Sets all the vertices "hasBeenDone" to 0 to prepare for new query
   */
  def resetDag(root: DagVertex): Unit = {
    val queue = collection.mutable.Queue[DagVertex]()
    root.hasBeenDone = 0
    queue.enqueue(root)
    while (queue.nonEmpty) {
      val deq_vert = queue.dequeue()
      deq_vert.children.foreach(child => {
        if (child._1.hasBeenDone == 1) {
          child._1.hasBeenDone = 0
          queue.enqueue(child._1)
        }
      })
    }
  }
}

/**
 * A vertex to be used in the DAG, represents a single projection
 *
 * @param p        The projection
 * @param p_length Its length
 * @param id       Its id (index in sequence of projections)
 */
class DagVertex(val p: Seq[Int], val p_length: Int, val id: Int) {
  var children = new ListBuffer[(DagVertex, Seq[Int])]()
  var hasBeenDone = 0

  /**
   * Adds a child to the vertex
   *
   * @param v the vertex of the child to add
   */
  def addChild(v: DagVertex): Unit = {
    val child_diff = p.filter(dim => !v.p.contains(dim))
    //println(test + "\n")
    children += ((v, child_diff))

  }
}