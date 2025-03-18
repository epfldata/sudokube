package experiments

import util.{ProgressIndicator, Util}

import java.io.{File, PrintStream}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random

object ProjectionSizesExperiment {

  def generateRandomKeys(d0: Int, d: Int) = {
    assert(d > 0)
    assert(d <= 31)
    assert(d0 <= d)
    val n0 = (1 << d0)
    val n = (1 << d)
    Util.collect_n(n0, () => Random.nextInt(n))
  }

  def generateRandom64(d0: Int) = {
    val n0 = (1 << d0)
    Util.collect_n(n0, () => Random.nextLong())
  }

  def projectKeys(mask: Int)(i: Int) = {
    i & mask // no need to shift the bits
    //BitUtils.projectIntWithInt(i, mask)
  }

  def projectKeyLong(mask: Long, i: Long) = mask & i

  def genMask(k: Int, d: Int) = {
    val bitpos = Util.collect_n(k, () => Random.nextInt(d))
    bitpos.map(i => 1L << i).sum
  }

  //n1-- how many times base cuboid is generated
  //n2-- how many different projections are considered
  def projectionSize(k: Int, d0: Int, d: Int)(iter1: Int, iter2: Int) = {
    var count = 0.0
    var total = 0L
    (0 until iter1).foreach { i =>
      val keys = generateRandomKeys(d0, d).toSet
      (0 until iter2).foreach { j =>
        val mask = genMask(k, d).toInt
        val projKeys = keys.map(projectKeys(mask)(_))
        total += projKeys.size
        count += 1.0
      }
    }
    total / count
  }

  def projectionSize64(k: Int, d0: Int)(iter1: Int, iter2: Int) = {
    var count = 0.0
    var total = 0L
    val d = 64
    (0 until iter1).foreach { i =>
      val keys = generateRandom64(d0).toSet
      (0 until iter2).foreach { j =>
        val mask = genMask(k, d)
        val projKeys = keys.par.map(projectKeyLong(mask, _))
        total += projKeys.size
        count += 1.0
      }
    }
    total / count
  }


  def estimatedProjectionSize(k: Int, d0: Int) = {
    import math._
    val power = pow(2, d0 - k)
    val density = (1 - exp(-power))
    (1 << k) * density
  }

  def run_expt_fixed_d_d0(iter1: Int, iter2: Int) = {
    val d = 64
    val d0 = 20
    val fileout = new File(s"expdata/thesis/fixed_d-${d}_d0-${d0}.csv")
    if (!fileout.exists) fileout.getParentFile.mkdirs()
    val out = new PrintStream(fileout)
    val header = "d,d0,k,n1,n2,Size,EstimatedSize,RelativeSizeToBase,EstimatedRelativeSizeToBase,Density,EstimatedDensity"
    out.println(header)

    val range = (6 until 30)
    val pi = new ProgressIndicator( range.size, "Simulated vs Estimated Fixed d d0 expt")
    range.reverse.map{ k =>
      val simulatedSize = projectionSize64(k, d0)(iter1, iter2)
      val estimatedSize = estimatedProjectionSize(k, d0)

      val baseFactor = math.pow(2, -d0)
      val simulatedRelativeSize = simulatedSize * baseFactor
      val estimatedRelativeSize = estimatedSize * baseFactor

      val thisCuboidFactor = math.pow(2, -k)
      val simulatedDensity = simulatedSize * thisCuboidFactor
      val estimatedDensity = estimatedSize * thisCuboidFactor
      pi.step
      s"$d,$d0,$k,$iter1,$iter2,$simulatedSize,$estimatedSize,$simulatedRelativeSize,$estimatedRelativeSize,$simulatedDensity,$estimatedDensity"
    }.foreach(out.println)
  }

  def run_expt_fixed_k(iter1: Int, iter2: Int) = {
    val fileout = new File("expdata/thesis/fixed_k.csv")
    if (!fileout.exists) fileout.getParentFile.mkdirs()
    val out = new PrintStream(fileout)
    val header = "d,d0,k,n1,n2,Size,RelativeSizeToBase,Density"
    out.println(header)

    val outerRange = (6 to 30)
    val s = outerRange.size
    val pi = new ProgressIndicator(s * (s + 1) / 2, s"Fixed k Expt ")
    implicit val ec = ExecutionContext.global
    val futs = outerRange.reverse.flatMap { d =>
      val innerrange = (6 to d)
      innerrange.reverse.map { d0 =>
        Future {
          val k = d0
          val size = projectionSize(k, d0, d)(iter1, iter2)
          val relativeToBase = size * math.pow(2, -d0)
          val density = size * math.pow(2, -k)
          //both are same here
          pi.step
          s"$d,$d0,$k,$iter1,$iter2,${size},${relativeToBase},$density"
        }
      }
    }
    val res = Await.result(Future.sequence(futs), Duration.Inf)
    res.foreach(out.println)
  }
  def main(args: Array[String]): Unit = {
    val iter1 = 1
    val iter2 = 1000
    //run_expt_fixed_k(iter1, iter2)
    run_expt_fixed_d_d0(iter1, iter2)
  }

}
