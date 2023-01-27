package experiments

import backend.CBackend
import frontend.generators.{CubeGenerator, NYC, RandomCubeGenerator, SSB, SSBSample}


/**
 * Experimenter to see how different scoring functions perform w.r.t. the solve time and solution error of the
 * IPF and moment based solver.
 * Different runs will generate different queries.
 * @author Thomas Depian
 */
object ScoringFunctionsPerformanceExperimenter {
  implicit val backend = CBackend.default

  /**
   * Run the experiment for a specific setup.
   * @note Depending on the mode, some parts of the SudoKube system must be adapted
   *       to match the sematnics of the mode, before running the experiment (e.g., turn subsumption-checks of)
   *
   * @param cgName Name of the cube generator used to generate the base cuboid.
   * @param d Dimensionality of the base cuboid.
   * @param d0 Sparsity of the base cuboid (there are pow(2, d0)-many non-zero entries).
   * @param b Materialization budget.
   * @param q Query dimensionality.
   * @param runs States how many runs (=queries) should be performed with that setup
   */
  def runScoringFunctionsPerformanceExperiment(cgName: String, d: Int, d0: Int, b: Double, q: Int, runs: Int, mode: String)(implicit shouldRecord: Boolean): Unit = {
    val cg: CubeGenerator = cgName match {
      case "NYC" => new NYC()
      case "RANDOM" => RandomCubeGenerator(d, d0)
      case "SSB-1" => new SSB(1)
      case "SSBSample" => new SSBSample(d0)
    }
    val base = cg.loadBase()
    val expname2 = s"scoring-functions-performance-$cgName-prefix-$d-$d0-$b-$q-${runs}_$mode"
    val queries = Array.ofDim[IndexedSeq[Int]](runs)
    val trueResults = Array.ofDim[Array[Double]](runs)
    (0 until runs).foreach(r => {
      val query = cg.schemaInstance.root.samplePrefix(q).sorted//scala.util.Random.shuffle((0 until d).toList).take(q).toIndexedSeq.sorted
      val trueResult = base.naive_eval(query)
      queries(r) = query
      trueResults(r) = trueResult
    })
    val exptfull = new ScoringFunctionsPerformanceExpt(expname2)
    exptfull.runAll(cg, d0, b, "", queries, trueResults, output = true, qname = "", sliceValues = Vector())
  }

  def main(args: Array[String]): Unit = {
    implicit val shouldRecord: Boolean = true

    //runScoringFunctionsPerformanceExperiment("RANDOM", d = 100, d0 = 20, b = 0.85, q = 8, runs = 50, "score_no_subsumption_solver_subsumption")
    //runScoringFunctionsPerformanceExperiment("RANDOM", d = 100, d0 = 20, b = 0.85, q = 10, runs = 50, "score_no_subsumption_solver_subsumption")
    //runScoringFunctionsPerformanceExperiment("RANDOM", d = 100, d0 = 20, b = 0.85, q = 12, runs = 50, "score_no_subsumption_solver_subsumption")
    //runScoringFunctionsPerformanceExperiment("RANDOM", d = 100, d0 = 20, b = 0.85, q = 14, runs = 50, "score_no_subsumption_solver_subsumption")
    //
    //runScoringFunctionsPerformanceExperiment("RANDOM", d = 120, d0 = 22, b = 0.75, q = 8, runs = 50, "score_no_subsumption_solver_subsumption")
    //runScoringFunctionsPerformanceExperiment("RANDOM", d = 120, d0 = 22, b = 0.75, q = 10, runs = 50, "score_no_subsumption_solver_subsumption")
    //runScoringFunctionsPerformanceExperiment("RANDOM", d = 120, d0 = 22, b = 0.75, q = 12, runs = 50, "score_no_subsumption_solver_subsumption")
    //runScoringFunctionsPerformanceExperiment("RANDOM", d = 120, d0 = 22, b = 0.75, q = 14, runs = 50, "score_no_subsumption_solver_subsumption")
    //runScoringFunctionsPerformanceExperiment("RANDOM", d = 120, d0 = 22, b = 0.75, q = 16, runs = 50, "score_no_subsumption_solver_subsumption")
    //
    //runScoringFunctionsPerformanceExperiment("RANDOM", d = 100, d0 = 16, b = 0.85, q = 8, runs = 50, "score_no_subsumption_solver_subsumption")
    //runScoringFunctionsPerformanceExperiment("RANDOM", d = 100, d0 = 16, b = 0.85, q = 10, runs = 50, "score_no_subsumption_solver_subsumption")
    //runScoringFunctionsPerformanceExperiment("RANDOM", d = 100, d0 = 16, b = 0.85, q = 12, runs = 50, "score_no_subsumption_solver_subsumption")
    //runScoringFunctionsPerformanceExperiment("RANDOM", d = 100, d0 = 16, b = 0.85, q = 14, runs = 50, "score_no_subsumption_solver_subsumption")
    //
    //runScoringFunctionsPerformanceExperiment("RANDOM", d = 150, d0 = 20, b = 0.85, q = 8, runs = 50, "score_no_subsumption_solver_subsumption")
    //runScoringFunctionsPerformanceExperiment("RANDOM", d = 150, d0 = 20, b = 0.85, q = 10, runs = 50, "score_no_subsumption_solver_subsumption")
    //runScoringFunctionsPerformanceExperiment("RANDOM", d = 150, d0 = 20, b = 0.85, q = 12, runs = 50, "score_no_subsumption_solver_subsumption")
    //runScoringFunctionsPerformanceExperiment("RANDOM", d = 150, d0 = 20, b = 0.85, q = 14, runs = 50, "score_no_subsumption_solver_subsumption")

    //runScoringFunctionsPerformanceExperiment("SSBSample", d = 188, d0 = 4, b = 0.25, q = 12, runs = 50, "score_no_subsumption_solver_subsumption")
    //runScoringFunctionsPerformanceExperiment("SSBSample", d = 188, d0 = 8, b = 0.25, q = 12, runs = 50, "score_no_subsumption_solver_subsumption")
    runScoringFunctionsPerformanceExperiment("SSB-1", d = 188, d0 = 23, b = 0.25, q = 9, runs = 50, "score_no_subsumption_solver_subsumption")
    runScoringFunctionsPerformanceExperiment("SSB-1", d = 188, d0 = 23, b = 0.25, q = 18, runs = 50, "score_no_subsumption_solver_subsumption")
    //runScoringFunctionsPerformanceExperiment("SSBSample", d = 188, d0 = 16, b = 0.25, q = 12, runs = 50, "score_no_subsumption_solver_subsumption")
    //runScoringFunctionsPerformanceExperiment("SSBSample", d = 188, d0 = 20, b = 0.25, q = 12, runs = 50, "score_no_subsumption_solver_subsumption")

    //runScoringFunctionsPerformanceExperiment("SSB-1", d = 188, d0 = 23, b = 0.25, q = 8, runs = 20, "score_no_subsumption_solver_subsumption")
    //runScoringFunctionsPerformanceExperiment("SSB-1", d = 188, d0 = 23, b = 0.25, q = 10, runs = 20, "score_no_subsumption_solver_subsumption")
    //runScoringFunctionsPerformanceExperiment("SSB-1", d = 188, d0 = 23, b = 0.25, q = 12, runs = 20, "score_no_subsumption_solver_subsumption")
    //runScoringFunctionsPerformanceExperiment("SSB-1", d = 188, d0 = 23, b = 0.25, q = 14, runs = 20, "score_no_subsumption_solver_subsumption")

//    runScoringFunctionsPerformanceExperiment("NYC", d = NYC.schemaInstance.n_bits, d0 = 27, b = 0.005, q = 10, runs = 50, "no_subsumption_check")
//    runScoringFunctionsPerformanceExperiment("NYC", d = NYC.schemaInstance.n_bits, d0 = 27, b = 0.005, q = 12, runs = 50, "no_subsumption_check")
//    runScoringFunctionsPerformanceExperiment("NYC", d = NYC.schemaInstance.n_bits, d0 = 27, b = 0.005, q = 15, runs = 50, "no_subsumption_check")
//    runScoringFunctionsPerformanceExperiment("NYC", d = NYC.schemaInstance.n_bits, d0 = 27, b = 0.005, q = 18, runs = 50, "no_subsumption_check")
//    runScoringFunctionsPerformanceExperiment("NYC", d = NYC.schemaInstance.n_bits, d0 = 27, b = 0.005, q = 21, runs = 50, "no_subsumption_check")
  }
}
