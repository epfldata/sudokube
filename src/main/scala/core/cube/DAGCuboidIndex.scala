package core.cube

import core.materialization.MaterializationScheme
import planning.{NewProjectionMetaData, ProjectionMetaData}
import util.Bits

import java.io.{ObjectInputStream, ObjectOutputStream}
import scala.collection.mutable.ListBuffer

/**
 * A directed acyclic graph representation of the projections, root is the full dimension projection. Has an edge from A to B if A.contains(B)
 */
class DAGCuboidIndex(val projectionsDAGroot: DagVertex, projections: IndexedSeq[IndexedSeq[Int]], override val n_bits: Int) extends CuboidIndex(n_bits) {
  override val typeName: String = "DAG"
  override protected def saveToOOS(oos: ObjectOutputStream): Unit = ???
  override def prepare(query: IndexedSeq[Int], cheap_size: Int, max_fetch_dim: Int): Seq[NewProjectionMetaData] = {
    val hm_cheap = collection.mutable.HashMap[Seq[Int], DagVertex]()
    val qSet = query.toSet
    val ret = new ListBuffer[NewProjectionMetaData]()
    val queue = collection.mutable.Queue[(DagVertex, Seq[Int])]()
    queue.enqueue((projectionsDAGroot, query))
    projectionsDAGroot.hasBeenDone = 1
    var i = 0
    while (queue.nonEmpty) {
      i += 1
      val (vert_deq, intersect_deq) = queue.dequeue()
      if (vert_deq.p_length >= max_fetch_dim) {
        vert_deq.children.foreach(child => {
          if (child._1.hasBeenDone == 0) {
            child._1.hasBeenDone = 1
            val new_intersect = intersect_deq.filter(dim => !child._2.contains(dim))
            if (new_intersect.nonEmpty) {
              queue.enqueue((child._1, new_intersect))
            }
          }
        })
      } else if (vert_deq.p_length <= cheap_size) {
        //When we reach cheap size, is basically the same algorithm as prepare_new
        var good_children = 0
        //Still iterate through children since if one of the children has the same intersection then
        //it will dominate => reduce further computation
        vert_deq.children.foreach(child => {
          val newdif = intersect_deq.intersect(child._2)
          if (newdif.isEmpty && child._1.hasBeenDone == 0) {
            queue.enqueue((child._1, intersect_deq))
            child._1.hasBeenDone = 1
            good_children += 1
          } else {
            child._1.hasBeenDone = 1
            val new_intersect = intersect_deq.filter(dim => !newdif.contains(dim))
            if (new_intersect.nonEmpty) {
              queue.enqueue((child._1, new_intersect))
            }
          }
        })
        if (good_children == 0) {
          val res = hm_cheap.get(intersect_deq)
          if (res.isDefined) {
            if (vert_deq.p_length < res.get.p_length) {
              hm_cheap(intersect_deq) = vert_deq
            }
          } else {
            hm_cheap(intersect_deq) = vert_deq
          }
        }
      } else {
        var good_children = 0
        vert_deq.children.foreach(child => {
          val newdif = intersect_deq.intersect(child._2)
          if (newdif.isEmpty) {
            if (child._1.hasBeenDone == 0) {
              queue.enqueue((child._1, intersect_deq))
              child._1.hasBeenDone = 1
            }
            good_children += 1
          } else {
            if (child._1.hasBeenDone == 0) {
              val new_intersect = intersect_deq.filter(dim => !newdif.contains(dim))
              if (new_intersect.nonEmpty) {
                queue.enqueue((child._1, intersect_deq.filter(dim => !newdif.contains(dim))))
              }
              child._1.hasBeenDone = 1
            }
          }
        })
        if (good_children == 0) {
          val ab0 = intersect_deq.toList
          val ab = query.indices.filter(i => ab0.contains(query(i)))
          val bitpos = Bits.mk_list_bitpos(vert_deq.p, qSet)
          val abInt = Bits.toInt(ab)
          ret += NewProjectionMetaData(abInt, vert_deq.id, vert_deq.p.length, bitpos)
        }
      }
    }
    //Add all cheap projs to ret
    hm_cheap.foreach({ case (ab0, dv) =>
      val ab = query.indices.filter(i => ab0.contains(query(i)))
      val abInt = Bits.toInt(ab)
      val bitpos = Bits.mk_list_bitpos(dv.p, qSet)
      ret += NewProjectionMetaData(abInt, dv.id, dv.p.length, bitpos)
    })
    resetDag(projectionsDAGroot)
    ret
  }

  override def qproject(query: IndexedSeq[Int], max_fetch_dim: Int): Seq[NewProjectionMetaData] = ???
  override def eliminateRedundant(cubs: Seq[NewProjectionMetaData], cheap_size: Int): Seq[NewProjectionMetaData] = ???


  override def length: Int = projections.length
  override def apply(idx: Int): IndexedSeq[Int] = projections.apply(idx)
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
class DagVertex(val p: IndexedSeq[Int], val p_length: Int, val id: Int) {
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

object DAGCuboidIndexFactory extends CuboidIndexFactory {
  /**
   * @return The root of the DAG
   */
  def buildDag(proj_zip_sorted: IndexedSeq[(IndexedSeq[Int], Int)]): DagVertex = {
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
    if (addedVtcs != proj_zip_sorted.length) {
      println("Error, not all vertices were added.")
    }
    root
  }
  override def buildFrom(m: MaterializationScheme): CuboidIndex = new DAGCuboidIndex(buildDag(m.projections.zipWithIndex.sortBy(-_._1.length)), m.projections, m.n_bits)
  override def loadFromOIS(ois: ObjectInputStream): CuboidIndex = ???
}