package experiments.thesis.infeasibility

import backend.CBackend
import core.DataCube
import experiments.{Experiment, ExperimentRunner}
import frontend.experiments.Tools
import frontend.generators._
import util.ProgressIndicator

class RandomProjectionExperiment(ename2: String = "")(implicit timestampedFolder: String, numIters: Int) extends Experiment(ename2, s"projection-size",  InfeasibilityExperimenter.folder) {

  import InfeasibilityExperimenter.be

  def numCuboids(k: Int, d0: Int, b: Int, total: Double) = {
    //s(k) + n(k) = d0 + b
    //s(k) = 2^k k < d0 else 2^d0
    val s = if (k < d0) k else d0
    val n = math.pow(2, d0 + b - s)
    n min total
  }
  def estimatedProjectionSize(k: Int, d0: Int) = {
    import math._
    val power = pow(2, d0 - k)
    val density = (1 - exp(-power))
    math.pow(2, k) * density
  }
  def simulatedProjectionSize[I](k: Int, dc: DataCube, cg: CubeGenerator[I], isPrefix: Boolean) = {
    val pi = new ProgressIndicator(numIters, s"Projecting to ${k} dimensions from ${dc.index.n_bits}")
    val total = (0 until numIters).map { i =>
      val query = Tools.generateQuery(isPrefix, cg.schemaInstance, k)
      val l = dc.index.prepareNaive(query)
      val pm = l.head
      val c = dc.cuboids(pm.cuboidID).rehash_to_sparse(pm.cuboidIntersection).asInstanceOf[be.SparseCuboid] //explicitly projecting to sparse
      val result = c.size
      be.cuboidGC(c.data) //Delete the cuboid immediately
      pi.step
      result
    }.sum.toDouble
    total / numIters
  }
  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean, qname: String, sliceValues: Seq[(Int, Int)]): Unit = ???
}

object InfeasibilityExperimenter extends ExperimentRunner{
  implicit val be = CBackend.default
  val folder = "thesis/infeasibility"

  def simulatedSizeOfd0Projection(implicit timestampFolder: String, numIters: Int) = {
    val expt = new RandomProjectionExperiment("kEqualsd0")
    expt.fileout.println("d,d0,numIters,Density")
    (6 to 23).reverse.foreach { d0 =>
      (d0 to 30).reverse.foreach { d =>
        val cg = new RandomCubeGenerator(d, d0)
        val dc = cg.loadBase(true)
        val size = expt.simulatedProjectionSize(d0, dc, cg, false)
        val ratio = size * math.pow(2, -d0)
        expt.fileout.println(s"$d,${d0},${numIters},$ratio")
        be.reset
      }
    }
  }

  def compareSimulatedWithExpectedSize(d: Int, d0: Int)(implicit timestampFolder: String, numIters: Int) = {
    val expt = new RandomProjectionExperiment("SimVsEst")
    expt.fileout.println("d,d0,k,numIters,SimulatedSize,EstimatedSize,SimulatedRelativeSize,EstimatedRelativeSize,SimulatedDensity,EstimatedDensity")
    val cg = new RandomCubeGenerator(d, d0)
    val dc = cg.loadBase(true)
    val range = (0 to (30 min d))

    range.reverse.foreach { k =>
      val simulatedSize = expt.simulatedProjectionSize(k, dc, cg, false)
      val estimatedSize = expt.estimatedProjectionSize(k, d0)
      val densityFactor = math.pow(2, -k)
      val baseCuboidFactor = math.pow(2, -d0)
      val simulatedRelativeSize = simulatedSize * baseCuboidFactor
      val estimatedRelativeSize = estimatedSize * baseCuboidFactor
      val simulatedDensity = simulatedSize * densityFactor
      val estimatedDensity = estimatedSize * densityFactor
      expt.fileout.println(s"$d,$d0,$k,$numIters," +
        s"$simulatedSize,$estimatedSize," +
        s"$simulatedRelativeSize,$estimatedRelativeSize," +
        s"$simulatedDensity,$estimatedDensity")
    }
    be.reset
  }

  def compareRandomToReal[I](cg: CubeGenerator[I])(implicit timestampFolder: String, numIters: Int): Unit = {
    val dc = cg.loadBase()
    val d = cg.schemaInstance.n_bits
    val d0 = cg match {
      case n: NYC => 27
      case s: SSB => 29
    }
    val expt = new RandomProjectionExperiment(cg.inputname)
    val range = (0 to 30)
    expt.fileout.println("d,d0,k,numIters,SizeRMS,SizeSMS,EstimatedSize")
    range.reverse.foreach { k =>
      val estimatedSize = expt.estimatedProjectionSize(k, d0)
      val simulatedSizeRealRandom = expt.simulatedProjectionSize(k, dc, cg, false)
      val simulatedSizeRealPrefix = expt.simulatedProjectionSize(k, dc, cg, true)
      expt.fileout.println(s"$d,$d0,$k,$numIters,$simulatedSizeRealRandom,$simulatedSizeRealPrefix,$estimatedSize")
    }
    be.reset
  }

  def expectedSizeFixed_d(d: Int, d0range: Vector[Int])(implicit timestampFolder: String, numIters: Int) = {
    val expt = new RandomProjectionExperiment(s"fixed-d-$d")
    expt.fileout.println("d,d0,k,EstimatedSize,EstimatedRelativeSize,EstimatedDensity")
    d0range.map { d0 =>
      val cg = new RandomCubeGenerator(d, d0)
      val dc = cg.loadBase(true)
      (0 to 40).map { k =>
        val esize = expt.estimatedProjectionSize(k, d0)
        val relativeSize = esize * math.pow(2, -d0)
        val density = esize * math.pow(2, -k)
        expt.fileout.println(s"$d,$d0,$k,$esize,$relativeSize,$density")
      }
    }
  }


  def numberOfMaterializableCuboidsVary_d0(d: Int, b: Int, d0Range: Vector[Int])(implicit timestampFolder: String, numIters: Int) = {
    val expt = new RandomProjectionExperiment(s"number-of-cuboids-vary-d0-$d-$b")
    expt.fileout.println("d,d0,b,k,NumberOfCuboids")
    (0 to 40).map { k =>
      val total = combinatorics.Combinatorics.comb(d, k).toDouble
      expt.fileout.println(s"$d,total,$b,$k,$total")
      d0Range.foreach { d0 =>
        val num = expt.numCuboids(k, d0, b, total)
        expt.fileout.println(s"$d,$d0,$b,$k,$num")
      }
    }
  }

  def numberOfMaterializableCuboidsVary_b(d: Int, d0: Int, bRange: Vector[Int])(implicit timestampFolder: String, numIters: Int) = {
    val expt = new RandomProjectionExperiment(s"number-of-cuboids-vary-b-$d-$d0")
    expt.fileout.println("d,d0,b,k,NumberOfCuboids")
    (0 to 40).map { k =>
      val total = combinatorics.Combinatorics.comb(d, k).toDouble
      expt.fileout.println(s"$d,$d0,total,$k,$total")
      bRange.foreach { b =>
        val num = expt.numCuboids(k, d0, b, total)
        expt.fileout.println(s"$d,$d0,$b,$k,$num")
      }
    }
  }

  def fractionOfMaterializableCuboidsVary_d(b: Int, d0: Int, dRange: Vector[Int])(implicit timestampFolder: String, numIters: Int) = {
    val expt = new RandomProjectionExperiment(s"number-of-cuboids-vary-d-$b-$d0")
    expt.fileout.println("d,d0,b,k,FractionOfCuboids")
    dRange.foreach { d =>
      (0 to 40).map { k =>
        val total = combinatorics.Combinatorics.comb(d, k).toDouble
        val num = expt.numCuboids(k, d0, b, total)
        val ratio = num / total
        expt.fileout.println(s"$d,$d0,$b,$k,$ratio")
      }
    }
  }

  def main(args: Array[String]) = {

    def func(param: String)(timestamp: String, numIters: Int) = {
      implicit val ni = numIters
      implicit val ts = timestamp
      param match {
        case "simu63" => simulatedSizeOfd0Projection
        case "simVexp" => compareSimulatedWithExpectedSize(64, 20)
        case "expVnyc" => compareRandomToReal(new NYC)
        case "expVssb" => compareRandomToReal(new SSB(100))
        case "exp_d0_k" => expectedSizeFixed_d(64, (5 to 25).by(5).toVector)
        case "numCub_d0_SSB" => numberOfMaterializableCuboidsVary_d0(188, 0, (20 to 30).by(2).toVector)
        case "numCub_d0_NYC" => numberOfMaterializableCuboidsVary_d0(420, 0, (20 to 30).by(2).toVector)
        case "numCub_b_SSB" => numberOfMaterializableCuboidsVary_b(188, 29, (-10 to 0).by(2).toVector)
        case "numCub_b_NYC" => numberOfMaterializableCuboidsVary_b(420, 27, (-10 to 0).by(2).toVector)
        case "fracCub" => fractionOfMaterializableCuboidsVary_d(4, 25, (50 to 550).by(50).toVector)
        case s => throw new IllegalArgumentException(s)
      }
    }

    run_expt(func)(args)
  }
}
