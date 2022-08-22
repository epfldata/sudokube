package experiments

import combinatorics.Combinatorics
import java.io.{File, PrintStream}
import scala.collection.mutable
import scala.collection.BitSet
import scala.util.Random

object ScoringFunctionsExperimenter {

  def countDimensions(A: BitSet): Int = {
    A.size
  }

  def intersectWithQuery(query: BitSet, cuboids: Array[BitSet]): Array[BitSet] = {
    cuboids.map(c => query & c).distinct
  }


  def computeScorePower(query: BitSet, cuboids: Array[BitSet]): Double = {
    intersectWithQuery(query, cuboids).map(c => math.pow(2, countDimensions(c))).sum
  }

  def computeScoreDiffToQuery(q: Int, query: BitSet, cuboids: Array[BitSet]): Double = {
    intersectWithQuery(query, cuboids).map(c => math.pow(2, q) - math.pow(2, q - countDimensions(c))).sum
  }

  def computeScoreUncovered(q: Int, query: BitSet, cuboids: Array[BitSet]): Double = {
    val correctionTerm = (1 to q).map(i => Combinatorics.comb(q, i).toDouble * math.pow(2, i)).sum
    correctionTerm - query.subsets().filterNot(s => countDimensions(s) == 0 || cuboids.exists(c => s.subsetOf(c))).map(s => math.pow(2, countDimensions(s))).sum
  }

  def computeScorePowerNoSubsumption(query: BitSet, cuboids: Array[BitSet]): Double = {
    val intersections = intersectWithQuery(query, cuboids).sortBy(c => countDimensions(c))(Ordering.Int.reverse)
    var score  = 0.0
    for (i <- intersections.indices) {
      val currentIntersection = intersections(i)
      var takeIntersection = true
      for (j <- 0 until i) {
        if (currentIntersection.subsetOf(intersections(j))) {
          takeIntersection = false
        }
      }
      if (takeIntersection) {
        score += math.pow(2, countDimensions(currentIntersection))
      }
    }
    score
  }


  def materializeCuboidsInDimK(d: Int, k: Int, numCuboids: Int, randomGenerator: Random): Array[BitSet] = {
    val cuboids: Array[BitSet] = Array.ofDim[BitSet](numCuboids)
    for (i <- 0 until numCuboids) {
      val c = mutable.BitSet()
      while(c.size < k) {
        c.add(randomGenerator.nextInt(d))
      }
      cuboids(i) = BitSet() ++ c //BitSet() ++ randomGenerator.shuffle(feasibleDims).take(k)
    }
    cuboids
  }

  def numberMaterializableCuboidsInDimK(d: Int, d0: Int, b: Double, k: Int): Int = {
    val budgetConstraint = math.floor((b * math.pow(2, d0) / math.pow(2, k + 3)) * math.ceil((1.0 * (d + 64)) / 8 ))
    budgetConstraint.min(Combinatorics.comb(d, k).toDouble).toInt
  }

  def generateRandomQuery(d: Int, q: Int, randomGenerator: Random): BitSet = {
    val qTmp = mutable.BitSet()
    while(qTmp.size < q) {
      qTmp.add(randomGenerator.nextInt(d))
    }
    BitSet() ++ qTmp
  }

  def simulate(d: Int, d0: Int, b: Double, q: Int, rematerializeCuboids: Boolean, regenerateQueries: Boolean, runs: Int, writer: PrintStream): Unit = {
    val randomGenerator = new Random()

    // Initialize Scoring Helpers
    val scoresPower: mutable.Map[Int, Double] = mutable.Map()
    val scoresDiffToQuery: mutable.Map[Int, Double] = mutable.Map()
    val scoresUncovered: mutable.Map[Int, Double] = mutable.Map()
    val scoresPowerNoSubsumption: mutable.Map[Int, Double] = mutable.Map()

    // Initialize Cuboids Store
    val cuboidsPerDimension: Array[Int] = Array.ofDim[Int](d0 + 1)
    val materializedCuboids: Array[Array[BitSet]] = Array.ofDim[Array[BitSet]](d0 + 1)

    // Initialize rest
    (1 to d0).foreach(k => {
      scoresPower += (k -> 0.0)
      scoresDiffToQuery += (k -> 0.0)
      scoresUncovered += (k -> 0.0)
      scoresPowerNoSubsumption += (k -> 0.0)
      cuboidsPerDimension(k) = numberMaterializableCuboidsInDimK(d, d0, b, k)
    })

    if (!rematerializeCuboids) {
      println("Choose cuboids to materialize")

      (1 to d0).foreach(k => {
        println(s"\tStarting for dimensionality $k (${cuboidsPerDimension(k)} to materialize)")
        materializedCuboids(k) = materializeCuboidsInDimK(d, k, cuboidsPerDimension(k), randomGenerator)
        println(s"\tDimensionality $k done!")
      })
      println("All dimensionality done!")
    }

    var query: BitSet = null
    if (!regenerateQueries) {
      query = generateRandomQuery(d, q, randomGenerator)
    }

    println("Start Runs")
    println("Progress: ")
    (0 until runs).foreach(r => {
      println(s"\tRun $r...")

      if (regenerateQueries) {
        query = generateRandomQuery(d, q, randomGenerator)
      }

      (1 to d0).foreach(k => {
        val cuboids = if (rematerializeCuboids) materializeCuboidsInDimK(d, k, cuboidsPerDimension(k), randomGenerator) else materializedCuboids(k)
        scoresPower += k -> (scoresPower(k) + computeScorePower(query, cuboids))
        scoresDiffToQuery += k -> (scoresDiffToQuery(k) + computeScoreDiffToQuery(q, query, cuboids))
        scoresUncovered += k -> (scoresUncovered(k) + computeScoreUncovered(q, query, cuboids))
        scoresPowerNoSubsumption += k -> (scoresPowerNoSubsumption(k) + computeScorePowerNoSubsumption(query, cuboids))
        println(s"\t\tk=$k done!")
      })

    })
    println("Done")
    writer.println("k;#Materialized Cuboids;Score Power;Score Diff To Query;Score Uncovered;Score Power No Subsumption")
    (1 to d0).foreach(k => {
      writer.println(s"$k;${cuboidsPerDimension(k)};${scoresPower(k) / runs};${scoresDiffToQuery(k) / runs};${scoresUncovered(k) / runs};${scoresPowerNoSubsumption(k) / runs}")
    })
  }


  def main(args: Array[String]): Unit = {
    val d = 20
    val d0 = 10
    val q = 8
    val b = 1.05
    val rematerializeCuboids = false
    val regenerateQueries = true
    val runs = 10
    val writeToFile = false

    var writer = Console.out
    if (writeToFile) {
      val fileName = s"scoring-functions-comparison-$d-$d0-$q-$b-$rematerializeCuboids-$regenerateQueries-$runs.csv"
      val file = new File(s"./expdata/scoring/$fileName")
      writer = new PrintStream(file)
      println(s"Results are printed to $fileName")
    }

    simulate(d, d0, b, q, rematerializeCuboids, regenerateQueries, runs, writer)
  }
}
