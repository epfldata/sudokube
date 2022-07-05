import core.materialization.builder._
import core.materialization._
import core.prepare._
import frontend.experiments.Tools
import org.scalatest.{FlatSpec, Matchers}
import planning.ProjectionMetaData
import util.Profiler

class PrepareSpec extends FlatSpec with Matchers {

  def RMS(nbits: Int, dmin: Int, logncubs: Int, nq: Int, qs: Int, cheap: Int, maxFetch: Int): Unit = {
    val m = RandomizedMaterializationScheme2(nbits, logncubs, dmin + logncubs - 1, 0)
    (0 until nq).foreach{ i =>
      val q = Tools.rand_q(nbits, qs)
      val oldpo = Profiler("OldPrepareOnline"){ClassicPreparer.prepareOnline(m, q, cheap, maxFetch)}.map(p => ProjectionMetaData(p.accessible_bits, p.accessible_bits0.toList.sorted, p.mask, p.id)).sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      val newpo = Profiler("NewPrepareOnline"){Preparer.default.prepareOnline(m, q, cheap, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      assert(oldpo.sameElements(newpo))
      val oldpb = Profiler("OldPrepareOnline"){ClassicPreparer.prepareBatch(m, q, maxFetch)}.map(p => ProjectionMetaData(p.accessible_bits, p.accessible_bits0.toList.sorted, p.mask, p.id)).sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      val newpb = Profiler("NewPrepareOnline"){Preparer.default.prepareBatch(m, q, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      assert(oldpb.sameElements(newpb))
    }
    println("Time for RMS")
    Profiler.print()
  }

  def RMS2(nbits: Int, dmin: Int, logncubs: Int, nq: Int, qs: Int, cheap: Int, maxFetch: Int): Unit = {
    val m = RandomizedMaterializationScheme2(nbits, logncubs, dmin + logncubs - 1, 0)
    val m_trie = SetTrieMaterializationScheme(m)
    val m_DAG = DAGMaterializationScheme(m)

    (0 until nq).foreach{ i =>
      val q = Tools.rand_q(nbits, qs)
      //val testp = Profiler("DagPrepare"){DAGPreparer.prepareOnline(m_DAG, q, cheap, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      //val oldp = Profiler("OldPrepare"){ClassicPreparer.prepareOnline(m, q, cheap, maxFetch)}.map(p => ProjectionMetaData(p.accessible_bits, p.accessible_bits0.toList.sorted, p.mask, p.id)).sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      val onlinep = Profiler("OnlineNewPrepare"){SetTrieOnlinePrepare1.prepareOnline(m, q, cheap, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      //val onlinep_int = Profiler("OnlineNewIntPrepare"){SetTrieOnlinePrepare2.prepareOnline(m, q, cheap, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      //val onlinep_int2 = Profiler("OnlineNewIntPrepare2"){SetTrieOnlinePrepare3.prepareOnline(m, q, cheap, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      //val onlinep_int3 = Profiler("OnlineNewIntPrepare3"){SetTrieOnlinePrepare4.prepareOnline(m, q, cheap, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      //val optp = Profiler("OptPrepare"){SetTrieOnlinePrepare5.prepareOnline(m, q, cheap, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      //val newp = Profiler("NewPrepare"){SetTrieBatchPrepare1.prepareBatch(m, q, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      val testp = Profiler("Online new w/ SetTrieIntersect"){SetTrieMSPreparer.prepareOnline(m_trie, q, cheap, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      m_trie.proj_trie.hm = collection.mutable.HashMap[List[Int], (Int, Int, Seq[Int])]()
      println("Proj trie hm accesses: " + m_trie.proj_trie.hm_accesses)
      m_trie.proj_trie.hm_accesses = 0
      //m_trie.proj_trie.hm_accesses_1 = 0
      //val batch_newp = Profiler("NewNewPrepare"){SetTrieBatchPrepare3.prepareBatch(m, q, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      //print("NewNew len : " + batch_newp.length)
      //print(", New len : " + newp.length + "\n")
      //println("Testp : " + testp.length + ", Onlinep : " + onlinep.length)
      //val bp_reg = Profiler("Create Build Plan"){SimpleCubeBuilder.create_build_plan(m)}
      //val bp_trie = Profiler("Create Build Plan Trie"){TrieCubeBuilder.create_build_plan(m)}
      //val bp_mu = Profiler("Create Build Plan Multi"){ParallelCubeBuilder.create_build_plan(8, m)}
      println("Onlinep : " + onlinep.length)
      println("Testp : " + testp.length)
      //println("Onlinep_int: " + onlinep_int.length)
      //println("Onlinep_int2: " + onlinep_int2.length)
      //println("Onlinep_int3: " + onlinep_int3.length)

      //assert(onlinep_int.sameElements(onlinep_int2))

    }
    println("Time for RMS")
    Profiler.print()
  }

  "Old and New Prepare " should " match " in RMS(200, 15, 15, 100, 10, 40, 40)

  //"Old and Newest Prepare " should " match " in RMS2(400, 15, 17, 100, 1, 40, 50)
  //"Old and New Prepare " should " match " in RMS2(200, 15, 15, 100, 10, 40, 50)
  //dmin can increase for testing 19/20
  //"Old and New Prepare " should " match " in RMS2(10, 2, 3, 100, 5, 10, 10)

}
