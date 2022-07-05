package core.prepare

import core.materialization.{DAGMaterializationScheme, DagVertex, MaterializationScheme}
import planning.ProjectionMetaData
import util.Bits

import scala.collection.mutable.ListBuffer

object DAGPreparer extends Preparer {
  override def prepareOnline(m: MaterializationScheme, query: Seq[Int], cheap_size: Int, max_fetch_dim: Int): Seq[ProjectionMetaData] = {
    assert(m.isInstanceOf[DAGMaterializationScheme])
    val dagm = m.asInstanceOf[DAGMaterializationScheme]
    val qIS = query.toIndexedSeq
    val qBS = query.toSet
    val hm_cheap = collection.mutable.HashMap[Seq[Int], DagVertex]()

    val ret = new ListBuffer[ProjectionMetaData]()
    val queue = collection.mutable.Queue[(DagVertex, Seq[Int])]()
    queue.enqueue((dagm.projectionsDAGroot, query))
    dagm.projectionsDAGroot.hasBeenDone = 1
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
          val ab = qIS.indices.filter(i => ab0.contains(qIS(i)))
          val mask = Bits.mk_list_mask(vert_deq.p.toList, qBS)
          ret += ProjectionMetaData(ab, ab0, mask, vert_deq.id)
        }
      }
    }
    //Add all cheap projs to ret
    hm_cheap.foreach({ case (ab0, dv) =>
      val ab = qIS.indices.filter(i => ab0.contains(qIS(i)))
      val mask = Bits.mk_list_mask(dv.p.toList, qBS)
      ret += ProjectionMetaData(ab, ab0, mask, dv.id)
    })
    dagm.resetDag(dagm.projectionsDAGroot)
    ret.toList
  }
}
