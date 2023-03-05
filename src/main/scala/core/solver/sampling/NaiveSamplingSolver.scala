package core.solver.sampling

class NaiveSamplingSolver(val qsize: Int,  totalSamples: Double) {
  var numSamples = 0

  val N = 1 << qsize
  var solution = Array.fill(N)(0.0)
  val unnormalizedSolution= Array.fill(N)(0L)
  var sampleSum = 0.0
  var moments = Array.fill(qsize)(0.0)
  def addSample(samples: Array[Long]): Unit = {
    val thisSampleSize = samples.size >> 1
    numSamples +=  thisSampleSize
    (0 until thisSampleSize).foreach{ i =>
      val keyIdx = i << 1
      val valIdx = keyIdx + 1
      val key = samples(keyIdx).toInt
      val value = samples(valIdx)
      util.BitUtils.IntToSet(key).foreach{b => moments(b) += value}
      sampleSum += value
      unnormalizedSolution(key) += value
    }
  }
  def solve() = {
    val scale = totalSamples / numSamples
    val temp = unnormalizedSolution.map(x => x * scale)
    solution = temp
    solution
  }
}
