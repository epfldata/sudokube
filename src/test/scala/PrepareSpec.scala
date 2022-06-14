import core.RandomizedMaterializationScheme2
import core.DAGMaterializationScheme
import core.testMaterializationScheme
import core.EfficientMaterializationScheme
import frontend.experiments.Tools
import org.scalatest.{FlatSpec, Matchers}
import planning.ProjectionMetaData
import util.Profiler

class PrepareSpec extends FlatSpec with Matchers {

  def RMS(nbits: Int, dmin: Int, logncubs: Int, nq: Int, qs: Int, cheap: Int, maxFetch: Int): Unit = {
    val m = RandomizedMaterializationScheme2(nbits, logncubs, dmin + logncubs - 1, 0)
    val mtest = EfficientMaterializationScheme(m)
    //val m_DAG = DAGMaterializationScheme(m)
    //val m_new = testMaterializationScheme(m)

    (0 until nq).foreach{ i =>
      val q = Tools.rand_q(nbits, qs)
      //val testp = Profiler("DagPrepare"){m_DAG.prepare(q, cheap, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      //val oldp = Profiler("OldPrepare"){m.prepare_old(q, cheap, maxFetch)}.map(p => ProjectionMetaData(p.accessible_bits, p.accessible_bits0.toList.sorted, p.mask, p.id)).sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      val onlinep = Profiler("OnlineNewPrepare"){m.prepare_online_new(q, cheap, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      //val onlinep_int = Profiler("OnlineNewIntPrepare"){m.prepare_online_new_int(q, cheap, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      //val onlinep_int2 = Profiler("OnlineNewIntPrepare2"){m.prepare_online_new_int2(q, cheap, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      //val onlinep_int3 = Profiler("OnlineNewIntPrepare3"){m.prepare_online_new_int3(q, cheap, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      //val optp = Profiler("OptPrepare"){m.prepare_opt(q, cheap, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      //val newp = Profiler("NewPrepare"){m.prepare_new(q, cheap, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      val testp = Profiler("Online new w/ SetTrieIntersect"){mtest.prepare(q, cheap, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      mtest.proj_trie.hm = collection.mutable.HashMap[List[Int], (Int, Int, Seq[Int])]()
      println("Proj trie hm accesses: " + mtest.proj_trie.hm_accesses)
      mtest.proj_trie.hm_accesses = 0
      //mtest.proj_trie.hm_accesses_1 = 0
      //val batch_newp = Profiler("NewNewPrepare"){m.prepare_batch_new(q, cheap, maxFetch)}.sortBy(p => p.accessible_bits.mkString("") + "_" +p.mask.length + "_" + p.id)
      //print("NewNew len : " + batch_newp.length)
      //print(", New len : " + newp.length + "\n")
      //println("Testp : " + testp.length + ", Onlinep : " + onlinep.length)
      //val bp_reg = Profiler("Create Build Plan"){m.create_build_plan()}
      //val bp_trie = Profiler("Create Build Plan Trie"){m.create_build_plan_trie()}
      //val bp_mu = Profiler("Create Build Plan Multi"){m.create_parallel_build_plan(8)(true)}
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


  "Old and New Prepare " should " match " in RMS(400, 15, 17, 100, 1, 40, 50)
  //"Old and New Prepare " should " match " in RMS(200, 15, 15, 100, 10, 40, 50)
  //dmin can increase for testing 19/20
  //"Old and New Prepare " should " match " in RMS(10, 2, 3, 100, 5, 10, 10)

}
