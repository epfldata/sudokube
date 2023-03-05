package core.solver.sampling

import core.solver.iterativeProportionalFittingSolver.MSTVanillaIPFSolver

class IPFSamplingSolver(qsize: Int, version: String) extends MSTVanillaIPFSolver(qsize) {
  val sampleSolution = Array.fill(N)(0.0)
  var sampleSum = 0.0

  def addSample(samples: Array[Long]): Unit = {
    val numSamples = samples.length >> 1
    (0 until numSamples).foreach { i =>
      val keyIdx = i << 1
      val valIdx = keyIdx + 1
      val key = samples(keyIdx).toInt
      val value = samples(valIdx).toDouble
      sampleSolution(key) += value
      sampleSum += value
    }
  }

  override def solve() = {
    def newx(x: Double) = if (version == "nomix")
      if (sampleSum > 0)
        x / sampleSum
      else 0
    else
      (x + 1) / (sampleSum + N)
    totalDistribution = sampleSolution.map(x => newx(x))
    //println("Min = " + totalDistribution.min + " Max = " + totalDistribution.max + "  Sum =" + totalDistribution.sum + " Avg =" + totalDistribution.sum/totalDistribution.size)
    var totalDelta = 0.0
    numIterations = 0
    do {
      numIterations += 1
      //println(s"\t\t\tIteration $numIterations")
      totalDelta = iterativeUpdate(numIterations)
    } while (sampleSum > 0 && totalDelta >= convergenceThreshold * N * clusters.size)
    //println("NumIters = " + numIterations)
    getSolution
  }
}
