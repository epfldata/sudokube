package core.solver.wavelet

import util.Profiler

import scala.math.Ordering.Implicits.seqDerivedOrdering

class CoefficientSelectionWaveletSolver(override val querySize: Int,
                                        override val debug: Boolean = false)
  extends
    WaveletSolver[Double]("CoefficientSelectionWaveletSolver", querySize, debug) {


  override def solve(): Array[Double] = {
    val finalWavelet: Array[Double] = new Array(N)

    // target order is ascending variables
    val finalCuboidVariables = (0 until querySize).sorted(Ordering[Int])

    // multiple cuboids can have the same variable,
    // each corresponding wavelet will have coefficients for that variable
    // we need to combine or select the coefficients from all the wavelets

    // here we sort the stored cuboids by the variable seq in ascending order
    // eg: if we have [[0, 1, 4], [0, 1, 2], [1], [2, 3, 4], [2, 4], [1, 2]]
    // we will sort them to [[0, 1, 2], [0, 1, 4], [1], [1, 2], [2, 3, 4], [2, 4]]
    // then do a stable sort on the length of the element seq
    // we will finally get [[1], [1, 2], [2, 4], [0, 1, 2], [0, 1, 4], [2, 3, 4]]
    // thus the lower dimensional marginals will be overwritten by the higher dimensional marginals
    val sortedCuboids = Profiler(s"$solverName Coefficient Priority Selection") {
      cuboids.toSeq
        .sortBy(_._1)(seqDerivedOrdering)
        .sortBy(_._1.length)(Ordering[Int])
    }

    if (debug) {
      println(s"Coefficient priority: ")
      println(s"\t${
        sortedCuboids.reverse.zipWithIndex.map {
          case ((variables, _), index) => s"$index : (${variables.mkString(",")})"
        }.mkString("\n\t")
      }")
      println()
    }

    for (((variables, cuboid), index) <- sortedCuboids.zipWithIndex) {
      val wavelet: Array[Double] = Profiler(s"$solverName Forward Transform") {
        // transform cuboid to wavelet
        transformer.forward(cuboid)
      }

      val extrapolatedWavelet: Array[Double] = Profiler(s"$solverName Wavelet Extrapolation") {
        // extrapolate wavelet to the correct size, by filling the missing values with 0
        val extrapolatedWavelet = Array.fill(N)(0.0)
        System.arraycopy(wavelet, 0, extrapolatedWavelet, 0, wavelet.length)

        extrapolatedWavelet
      }



      // copy the coefficients to the final wavelet
      Profiler(s"$solverName Wavelet Permutation & Combination") {
        // copy the sum to the final wavelet
        if (index == 0) {
          finalWavelet(0) = extrapolatedWavelet(0)
        }

        // fill the wavelet values into the right positions in the final wavelet

        /* eg:
        querySize = 3
        extrapolatedWavelet (0, 2) :: (a, b, c, d)

        for variable = 2 (index 0): variablePos = 2^0 = 1, numCoefficients = 2^0 = 1, variableCoefficients = (b)
        for variable = 0 (index 1): variablePos = 2^1 = 2, numCoefficients = 2^1 = 2, variableCoefficients = (c, d)

        in final wavelet (0, 1, 2) :: (t, 1, 2, 3, 4, 5, 6, 7)

        for variable = 2: variablePos = 2^0 = 1, numCoefficients = 2^0 = 1, finalWavelet(1) = (b)
        for variable = 0: variablePos = 2^2 = 4, numCoefficients = 2^2 = 4, finalWavelet([4...7]) = (c/2, c/2, d/2, d/2)
        */
        // for each variable
        variables.reverse.zipWithIndex.foreach { case (variable, i) =>
          val variablePos = Math.pow(2, i).toInt
          val numCoefficients = variablePos

          // get all the coefficients of the variable
          val variableCoefficients = extrapolatedWavelet.slice(variablePos, variablePos + numCoefficients)

          // set the coefficients of the variable in the final wavelet
          val finalVariablePos = Math.pow(2, (querySize - 1) - variable).toInt
          val finalNumCoefficients = finalVariablePos

          var adjustedCoefficients = Array.empty[Double]

          if (numCoefficients == finalNumCoefficients) {
            // we can just copy the coefficients
            adjustedCoefficients = variableCoefficients

          } else if (numCoefficients < finalNumCoefficients) {
            // we need to equally split the coefficients, giving us double the number of coefficients
            val divisor = finalNumCoefficients / numCoefficients
            adjustedCoefficients = variableCoefficients.flatMap(c => Array.fill(divisor)(c / divisor))

          } else /* if numCoefficient > finalNumCoefficients  */ {
            // we need to sum the coefficients pairwise, giving us half the number of coefficients

            /*
            eg. given marginal source cuboid (2, 4, 5) :: [total, 5, 4_0, 4_1, 2_0, 2_1, 2_2, 2_3]
            the final cuboid will be (..., 2, ..., 4, 5, ...)
            In the final cuboid, 2 cannot have less number of variables succeeding it than it has in the marginal cuboid.
            Hence, 2 will never require less number of coefficients than it has in the marginal cuboid.
            */
            assert(assertion = false,
              "Unexpected condition: numCoefficients > finalNumCoefficients for \n" +
                s"MarginalCuboid: ${variables.mkString(", ")}")
          }

          System.arraycopy(adjustedCoefficients, 0, finalWavelet, finalVariablePos, finalNumCoefficients)
        }
      }

      if (debug) {
        println(s"Source Cuboid: $variables :: ${cuboid.mkString(", ")}")
        println(s"Source Wavelet:  $variables :: ${extrapolatedWavelet.mkString(", ")}")
        println(s"Combined Wavelet: $finalCuboidVariables :: ${finalWavelet.mkString(", ")}")
        println()
      }
    }

    val finalCuboid: Array[Double] = Profiler(s"$solverName Reverse Transform") {
      transformer.reverse(finalWavelet)
    }

    if (debug) {
      println(s"Final Wavelet: $finalCuboidVariables :: ${finalWavelet.mkString(", ")}")
      println(s"Final Cuboid: $finalCuboidVariables :: ${finalCuboid.mkString(", ")}")
      println()
    }

    solution = finalCuboid
    finalCuboid
  }
}
