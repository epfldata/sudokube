package experiments.thesis.backend

import backend.CBackend._
import backend.Cuboid
import core.DataCube
import experiments.{Experiment, ExperimentRunner}

import util.{BigBinary, Profiler}

import scala.util.Random

class BackendExperiment(ename2: String = "")(implicit timestampedFolder: String, numIters: Int) extends Experiment(ename2, s"projection-time", "thesis/backend") {
  val header = "SrcSize,d0,DstSize,BlkSize,BitPos,OriginalS2D,RowStoreS2D,ColStoreS2D,OriginalD2D,RowStoreD2D"
  fileout.println(header)

  def loadData(data: IndexedSeq[(BigBinary, Long)], nbits: Int) = {
    val origSparse = original.mkAll(nbits, data)
    val rowSparse = rowstore.mkAll(nbits, data)
    val colSparse = colstore.mkAll(nbits, data)
    val rowDense = if(nbits < 30) rowSparse.rehash_to_dense(0 until nbits) else null
    val origDense = if(nbits < 30) origSparse.rehash_to_dense(0 until nbits) else null
    Vector[Cuboid](origSparse, rowSparse, colSparse, origDense, rowDense)
  }
  def reset() = {
    original.reset
    rowstore.reset
    colstore.reset
  }

  def applyProjections(cuboids: Vector[Cuboid], bitpos: IndexedSeq[Int]) = {
    Profiler.resetAll()
    cuboids.zipWithIndex.map { case (cub, id) =>
      if(cub == null) Int.MinValue
      else {
        Profiler(id.toString) {
          cub.rehash_to_dense(bitpos)
        }
        Profiler.getDurationMicro(id.toString)
      }
    }
  }
  def genProjection(d: Int, k: Int, blkSize: Int) = {
      val set = collection.mutable.BitSet()
      while(set.size < k) {
        val dim = Random.nextInt(d)
        val size = blkSize min (k - set.size)
        set ++= (dim until ((dim + size) min d))
      }
    assert(set.size == k)
    assert(set.max < d)
    set.toIndexedSeq.sorted
  }
  def run2(data: IndexedSeq[(BigBinary, Long)], nbits: Int, d0: Int, blkSize: Int, bitpos: IndexedSeq[Int]) = {
    reset()
    val cubs = loadData(data, nbits)
    val times = applyProjections(cubs, bitpos)
    val row = s"$nbits,$d0,${bitpos.length},$blkSize," + bitpos.mkString( ";") + "," + times.mkString(",")
    fileout.println(row)
  }
  def genData(srcSize: Int, d0: Int) = {
    val res = if(srcSize >= d0 + 10) { //sample with replacement
      val n = 1 << d0
      (0 until n).toArray.map{i =>
        var remainingBits = srcSize
        var key = BigInt(0)
        while(remainingBits >= 30) {
          key = (key << 30) + Random.nextInt(1 << 30)
          remainingBits -= 30
        }
        while(remainingBits >= 8) {
          key = (key << 8) + Random.nextInt(1 << 8)
          remainingBits -= 8
        }
        while(remainingBits >= 0) {
          key = (key << 1) + Random.nextInt(2)
          remainingBits -= 1
        }
        BigBinary(key) -> 1L
      }
    } else if(srcSize < 30) {
      val n0 = 1 << srcSize
      //can't use random.shuffle, too inefficient.
      val array = new Array[Int](n0)
      var idx = 0
      while (idx < n0) {
        array(idx) = idx
        idx += 1
      }
      var n = n0
      while (n >= 2) {
        val j = Random.nextInt(n)
        val temp = array(n - 1)
        array(n - 1) = array(j)
        array(j) = temp
        n -= 1
      }
     array.take(1 << d0).map{i => BigBinary(i) -> 1L}
    } else ???

    assert(res.size == (1 << d0))
   res
  }
  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean, qname: String, sliceValues: Seq[(Int, Int)]): Unit = ???
}

object BackendExperiment extends ExperimentRunner {
  def sourceSizeSparse()(implicit timestampFolder: String, numIters: Int) = {
    val dstSize = 10
    val blkSize = 1
    val d0 = 20
    val expt = new BackendExperiment("source-dim-sparse")
    Vector(20, 80, 140, 200).foreach{ srcSize =>
      val data = expt.genData(srcSize, d0)
      (0 until numIters).foreach { i =>
        expt.run2(data, srcSize, d0, blkSize, expt.genProjection(srcSize, dstSize, blkSize))
      }
    }
  }

  def sourceSizeDense()(implicit timestampFolder: String, numIters: Int) = {
    val dstSize = 10
    val blkSize = 1
    val d0 = 12
    val expt = new BackendExperiment("source-dim-dense")
    Vector(12, 16, 20, 24).foreach { srcSize =>
      val data = expt.genData(srcSize, d0)
      (0 until numIters).foreach { i =>
        expt.run2(data, srcSize, d0, blkSize, expt.genProjection(srcSize, dstSize, blkSize))
      }
    }
  }

  def dstSizeSparse()(implicit timestampFolder: String, numIters: Int) = {
    val srcSize = 200
    val d0 = 20
    val blkSize = 1
    val expt = new BackendExperiment("dest-dim-sparse")
    val data = expt.genData(srcSize, d0)
    Vector(6, 10, 14, 18).foreach { dstSize =>
      (0 until numIters).foreach { i =>
        expt.run2(data, srcSize, d0, blkSize, expt.genProjection(srcSize, dstSize, blkSize))
      }
    }
  }

  def dstSizeDense()(implicit timestampFolder: String, numIters: Int) = {
    val srcSize = 20
    val d0 = 20
    val blkSize = 1
    val expt = new BackendExperiment("dest-dim-dense")
    val data = expt.genData(srcSize, d0)
    Vector(6, 10, 14, 18).foreach { dstSize =>
      (0 until numIters).foreach { i =>
        expt.run2(data, srcSize, d0, blkSize, expt.genProjection(srcSize, dstSize, blkSize))
      }
    }
  }

  def sparsitySparse()(implicit timestampFolder: String, numIters: Int) = {
    val srcSize = 200
    val blkSize = 1
    val dstSize = 10
    val expt = new BackendExperiment("sparsity-sparse")
    Vector(5, 10, 15, 20).foreach { d0 =>
      val data = expt.genData(srcSize, d0)
      (0 until numIters).foreach { i =>
        expt.run2(data, srcSize, d0, blkSize, expt.genProjection(srcSize, dstSize, blkSize))
      }
    }
  }

  def sparsityDense()(implicit timestampFolder: String, numIters: Int) = {
    val srcSize = 20
    val blkSize = 1
    val dstSize = 10
    val expt = new BackendExperiment("sparsity-dense")
    Vector(5, 10, 15, 20).foreach { d0 =>
      val data = expt.genData(srcSize, d0)
      (0 until numIters).foreach { i =>
        expt.run2(data, srcSize, d0, blkSize, expt.genProjection(srcSize, dstSize, blkSize))
      }
    }
  }

  def blockSizeSparse()(implicit timestampFolder: String, numIters: Int) = {
    val srcSize = 200
    val dstSize = 10
    val d0 = 20
    val expt = new BackendExperiment("block-size-sparse")
    Vector(1, 2, 3, 4, 5).foreach { blkSize =>
      val data = expt.genData(srcSize, d0)
      (0 until numIters).foreach { i =>
        expt.run2(data, srcSize, d0, blkSize, expt.genProjection(srcSize, dstSize, blkSize))
      }
    }
  }

  def blockSizeDense()(implicit timestampFolder: String, numIters: Int) = {
    val srcSize = 20
    val dstSize = 10
    val d0 = 20
    val expt = new BackendExperiment("block-size-dense")
    Vector(1, 2, 3, 4, 5).foreach { blkSize =>
      (0 until numIters).foreach { i =>
        val data = expt.genData(srcSize, d0)
        expt.run2(data, srcSize, d0, blkSize, expt.genProjection(srcSize, dstSize, blkSize))
      }
    }
  }

  def main(args: Array[String]): Unit = {
    def func(param: String)(timestamp: String, numIters: Int) = {
      implicit val ni = numIters
      implicit val ts = timestamp
      param match {
        case "source-size-sparse" => sourceSizeSparse()
        case "source-size-dense" => sourceSizeDense()
        case "dest-size-sparse" => dstSizeSparse()
        case "dest-size-dense" => dstSizeDense()
        case "sparsity-sparse" => sparsitySparse()
        case "sparsity-dense" => sparsityDense()
        //case "block-size-sparse" => blockSizeSparse()
        //case "block-size-dense" => blockSizeDense()
        case s => throw new IllegalArgumentException(s)
      }
    }
    run_expt(func)(args)
  }
}