package experiments

import combinatorics.Combinatorics

import java.io.{File, PrintStream}
import scala.collection.mutable
import scala.collection.BitSet
import scala.util.Random


/**
 * Experimenter to see which behavior of the scoring functions we can expect for different setups.
 * The simulated cuboids and query are implemented as a bitset.
 * To simulate a materialization we store the dimensions of a cuboid, to compute the score
 * for a potential query, we compute the intersection of the query with the cuboid.
 * Hence, no cuboid is actually materialized and no solver is involved.
 *
 * In the documentation, we use C_Q to denote the set of materialized cuboids, where each materialized
 * cuboid C was intersected with the query Q.
 *
 * @author Thomas Depian
 */
object ScoringFunctionsExperimenter {

  /**
   * Counts the dimensions in cuboid C.
   * A query Q can also be seen as a cuboid C.
   * @param C Cuboid of which we want to count the dimensions.
   * @return The number of dimensions this cuboid contains.
   */
  def countDimensions(C: BitSet): Int = {
    C.size
  }

  /**
   * Intersects a set of materialized cuboids with a query and removes duplicates.
   * @param query The query with which the cuboids should be intersected.
   * @param cuboids The "materialized" cuboids.
   * @return Returns an array of cuboids, where each cuboid was intersected with the query.
   *         Duplicates that might occur are removed.
   */
  def intersectWithQuery(query: BitSet, cuboids: Array[BitSet]): Array[BitSet] = {
    cuboids.map(c => query & c).distinct
  }


  /**
   * Computes the "power score" for a query and a set of cuboids.
   * This score is defined to be "sum over all C in C_Q: pow(2, |C|)".
   * @param query The query for which the score should be computed.
   * @param cuboids The "materialized" cuboids.
   * @return The computed score.
   */
  def computeScorePower(query: BitSet, cuboids: Array[BitSet]): Double = {
    intersectWithQuery(query, cuboids).map(c => math.pow(2, countDimensions(c))).sum
  }

  /**
   * Computes the "diff to query score" for a query and a set of cuboids.
   * This score is defined to be "sum over all C in C_Q: pow(2, q) - pow(2, |C|)".
   * @param q The size of the query.
   * @param query The query for which the score should be computed.
   * @param cuboids The "materialized" cuboids.
   * @return The computed score.
   */
  def computeScoreDiffToQuery(q: Int, query: BitSet, cuboids: Array[BitSet]): Double = {
    intersectWithQuery(query, cuboids).map(c => math.pow(2, q) - math.pow(2, q - countDimensions(c))).sum
  }

  /**
   * Computes the "uncovered score" for a query and a set of cuboids.
   * This score is defined to be "sum over all non-empty subsets S of Q: pow(2, |S|), if there is no C in C_Q that is a superset of S; 0 otherwise".
   * We subtract this score from the largest possible score in order to let higher scores be better.
   * @param q The size of the query.
   * @param query The query for which the score should be computed.
   * @param cuboids The "materialized" cuboids.
   * @return The computed score.
   */
  def computeScoreUncovered(q: Int, query: BitSet, cuboids: Array[BitSet]): Double = {
    val correctionTerm = (1 to q).map(i => Combinatorics.comb(q, i).toDouble * math.pow(2, i)).sum
    correctionTerm - query.subsets().filterNot(s => countDimensions(s) == 0 || cuboids.exists(c => s.subsetOf(c))).map(s => math.pow(2, countDimensions(s))).sum
  }

  /**
   * Computes the "power score no subsumption" for a query and a set of cuboids.
   * This score has the same definition as the "normal" power score, but we ignore
   * now cuboids C that are subsumed by another cuboid C'.
   * @param query The query for which the score should be computed.
   * @param cuboids The "materialized" cuboids.
   * @return The computed score.
   */
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

  /**
   * Computes the "weighted power score" for a query and a set of cuboids.
   * This score is defined to be "sum over all C in C_Q: |C| * pow(2, |C|)".
   * @param query The query for which the score should be computed.
   * @param cuboids The "materialized" cuboids.
   * @return The computed score.
   */
  def computeScorePowerWeighted(query: BitSet, cuboids: Array[BitSet]): Double = {
    intersectWithQuery(query, cuboids).map(c => countDimensions(c) * math.pow(2, countDimensions(c))).sum
  }

  /**
   * Computes the "power score with the chance of intersection" for a query and a set of cuboids.
   * This score is defined to be "sum over all C in C_Q: 1/(k choose |C|) * pow(2, |C|)".
   * @param query The query for which the score should be computed.
   * @param cuboids The "materialized" cuboids.
   * @return The computed score.
   */
  def computeScorePowerChanceOfIntersection(query: BitSet, cuboids: Array[BitSet], k: Int): Double = {
    intersectWithQuery(query, cuboids).map(c => (1.0 / Combinatorics.comb(k, countDimensions(c)).toDouble) * math.pow(2, countDimensions(c))).sum
  }

  /**
   * Computes the "inclusion exclusion power score" for a query and a set of cuboids.
   * This score is defined to be "| Union over all C in C_Q: powerset(C)|".
   * @param query The query for which the score should be computed.
   * @param cuboids The "materialized" cuboids.
   * @return The computed score.
   */
  def computeScorePowerInclusionExclusion(query: BitSet, cuboids: Array[BitSet]): Double = {
    val powerSets = mutable.Set[BitSet]()
    intersectWithQuery(query, cuboids).map(c => c.subsets()).foreach(s => s.foreach(c => powerSets.add(c)))
    powerSets.size
  }


  /**
   * "Materialize" "numCuboids"-many cuboids with dimensionality k.
   * Materializing means here storing the selected dimensions as BitSet.
   * The cuboids are
   * @param d Dimensionality of the base cuboid.
   * @param k Dimensionality to materialize.
   * @param numCuboids Number of cuboids to materialize.
   * @param randomGenerator Random Generator used whenever we require a random number.
   * @return Returns an array of size NumCuboids full of cuboids.
   */
  def materializeCuboidsInDimK(d: Int, k: Int, numCuboids: Int, randomGenerator: Random): Array[BitSet] = {
    val cuboids: Array[BitSet] = Array.ofDim[BitSet](numCuboids)
    for (i <- 0 until numCuboids) {
      val c = mutable.BitSet()
      while(c.size < k) {
        c.add(randomGenerator.nextInt(d))
      }
      cuboids(i) = BitSet() ++ c
    }
    cuboids
  }


  /**
   * Computes the number of materializable cuboids with dimensionality k.
   * @param d Dimensionality of the base cuboid.
   * @param d0 Sparsity of the base cuboid (there are pow(2, d0)-many non-zero entries).
   * @param b Materialization budget.
   * @param k Dimensionality in which we want to materialize.
   * @return Returns the number of materializable cuboids.
   */
  def numberMaterializableCuboidsInDimK(d: Int, d0: Int, b: Double, k: Int): Int = {
    val budgetConstraint = math.floor((b * math.pow(2, d0) / math.pow(2, k + 3)) * math.ceil((1.0 * (d + 64)) / 8 ))
    budgetConstraint.min(Combinatorics.comb(d, k).toDouble).toInt
  }


  /**
   * Generates a random query of a given dimensionality.
   * @param d Dimensionality of the base cuboid.
   * @param q Dimensionality of the query.
   * @param randomGenerator Random Generator used whenever we require a random number.
   * @return The query encoded as BitSet.
   */
  def generateRandomQuery(d: Int, q: Int, randomGenerator: Random): BitSet = {
    val qTmp = mutable.BitSet()
    while(qTmp.size < q) {
      qTmp.add(randomGenerator.nextInt(d))
    }
    BitSet() ++ qTmp
  }

  /**
   * Runs the simulation.
   * See the documentation of the files for further information.
   * @param d Dimensionality of the base cuboid.
   * @param d0 Sparsity of the base cuboid (there are pow(2, d0)-many non-zero entries).
   * @param b Materialization budget.
   * @param q Dimensionality of the query.
   * @param rematerializeCuboids If true, after each run (query), a new set of cuboids will be materialized.
   *                             If false, only one set of cuboids will be materialized and will then be used throughout the experiments.
   * @param regenerateQueries If true, generate and use a new query for each run. If false, generate one query and use it for all runs.
   * @param runs Number of runs. The score will be averaged over all runs.
   * @param writer Write to which the results should be written to.
   */
  def simulate(d: Int, d0: Int, b: Double, q: Int, rematerializeCuboids: Boolean, regenerateQueries: Boolean, runs: Int, writer: PrintStream): Unit = {
    val randomGenerator = new Random()

    // Initialize Scoring Helpers
    val scoresPower: mutable.Map[Int, Double] = mutable.Map()
    val scoresDiffToQuery: mutable.Map[Int, Double] = mutable.Map()
    val scoresUncovered: mutable.Map[Int, Double] = mutable.Map()
    val scoresPowerNoSubsumption: mutable.Map[Int, Double] = mutable.Map()
    val scoresPowerWeighted: mutable.Map[Int, Double] = mutable.Map()
    val scoresPowerChanceOfIntersection: mutable.Map[Int, Double] = mutable.Map()
    val scoresPowerInclusionExclusion: mutable.Map[Int, Double] = mutable.Map()

    // Initialize Cuboids Store
    val cuboidsPerDimension: Array[Int] = Array.ofDim[Int](d0 + 1)
    val materializedCuboids: Array[Array[BitSet]] = Array.ofDim[Array[BitSet]](d0 + 1)

    // Initialize rest
    (1 to d0).foreach(k => {
      scoresPower += (k -> 0.0)
      scoresDiffToQuery += (k -> 0.0)
      scoresUncovered += (k -> 0.0)
      scoresPowerNoSubsumption += (k -> 0.0)
      scoresPowerWeighted += (k -> 0.0)
      scoresPowerChanceOfIntersection += (k -> 0.0)
      scoresPowerInclusionExclusion += (k -> 0.0)
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
        scoresPowerWeighted += k -> (scoresPowerWeighted(k) + computeScorePowerWeighted(query, cuboids))
        scoresPowerChanceOfIntersection += k -> (scoresPowerChanceOfIntersection(k) + computeScorePowerChanceOfIntersection(query, cuboids, k))
        scoresPowerInclusionExclusion += k -> (scoresPowerInclusionExclusion(k) + computeScorePowerInclusionExclusion(query, cuboids))
        println(s"\t\tk=$k done!")
      })

    })
    println("Done")
    writer.println("k;#Materialized Cuboids;Score Power;Score Diff To Query;Score Uncovered;Score Power No Subsumption;Score Power Weighted; Score Power Chance Intersection; Score Power Inclusion Exclusion")
    (1 to d0).foreach(k => {
      writer.println(s"$k;${cuboidsPerDimension(k)};${scoresPower(k) / runs};${scoresDiffToQuery(k) / runs};${scoresUncovered(k) / runs};${scoresPowerNoSubsumption(k) / runs};${scoresPowerWeighted(k) / runs};${scoresPowerChanceOfIntersection(k) / runs};${scoresPowerInclusionExclusion(k) / runs}")
    })
  }


  /**
   * Entry point of the experimenter.
   * Set the parameters (see documentation of simulate()-method).
   * If writeToFile is set to true, it will write (and overwrite!) the results to a file in "expdata".
   * The filename will be scoring-functions-comparison-[d]-[d0]-[q]-[b]-[rematerializeCuboids]-[regenerateQueries]-[runs].csv
   * If writeToFile is set to false, the results are displayed at the console.
   */
  def main(args: Array[String]): Unit = {
    val d = 100
    val d0 = 22
    val q = 10
    val b = 0.5
    val rematerializeCuboids = false
    val regenerateQueries = true
    val runs = 50
    val writeToFile = true

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
