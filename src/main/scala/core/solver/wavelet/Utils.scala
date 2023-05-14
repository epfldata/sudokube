package core.solver.wavelet

import core.solver.iterativeProportionalFittingSolver.IPFUtils
import util.BitUtils.SetToInt

import java.io.{File, PrintWriter}
import java.lang.Integer.parseInt
import scala.reflect.ClassTag
import scala.util.Random

object AnalyseTransformer {
  def main(args: Array[String]): Unit = {
    val rng = new Random(42)

    val writer = new PrintWriter(new File("./src/main/scala/core/solver/wavelet/wavelet-analysis.md"))

    val transformer = new HaarKroneckerTransformer[Double]()
    writer.write(s"## ${transformer.name}  \n")

    val querySizes = Seq(2, 3, 4, 5)
    querySizes.foreach { querySize =>
      val actual: Array[Double] = Array.fill(1 << querySize)(rng.nextInt(20))
      val actualWavelet: Array[Double] = transformer.forward(actual)

      // primary cuboid for querySize 3 is (0, 1, 2)
      val primaryCuboidShape: Seq[Int] = (0 until querySize)
      // all cuboids for querySize 3 are (0, 1), (0, 2), (1, 2), (0), (1), (2)
      val allCuboidShapes: Seq[Seq[Int]] = (1 until querySize).flatMap { n =>
        primaryCuboidShape.combinations(n).flatMap {
          cuboidOfSizeN => cuboidOfSizeN.permutations
        }
      }

      val allCuboids: Map[Seq[Int], Array[Double]] =
        allCuboidShapes.map(columnIndices =>
          columnIndices -> IPFUtils.getMarginalDistribution(querySize, actual, columnIndices.size, SetToInt(columnIndices))
        ).toMap

      val allWavelets = allCuboids.map { case (columnIndices, values) =>
        columnIndices -> (values, transformer.forward(values))
      }

      writer.write(s"### Query Size $querySize  \n")
      writer.write(s"    Primary: ${PrintUtils.toString(actual)}  \n")
      writer.write(s"    Wavelet: ${PrintUtils.toString(actualWavelet)}  \n")
      allWavelets.foreach { case (columnIndices, (cuboid, wavelet)) =>
        writer.write(s"a. ${columnIndices.mkString(",")}  \n")
        writer.write(s"    Original: ${PrintUtils.toString(cuboid)}  \n")
        writer.write(s"    Wavelet: ${PrintUtils.toString(wavelet)}  \n")
      }
    }
    writer.close()
  }
}

object PrintUtils {

  def toString(array: Array[Double], sep: String = " "): String =
    array.map { double => "%.2f".format(double) }.mkString(sep)

  def main(args: Array[String]): Unit = {
    implicit val op: OperationPrinter = new OperationPrinter()
    val t = new HaarTransformer[String]

    Seq(
      "0,1,2" -> Array("a", "b", "c", "d", "e", "f", "g", "h"),
      "0,1" -> Array("a+e", "b+f", "c+g", "d+h"),
      "0,2" -> Array("a+c", "b+d", "e+g", "f+h"),
      "0,1" -> Array("a+e", "b+f", "c+g", "d+h"),
      "1,2" -> Array("a+b", "c+d", "e+f", "g+h"),
      "0,2,1" -> Array("a", "b", "e", "f", "c", "d", "g", "h"),
      "2,0,1" -> Array("a", "e", "b", "f", "c", "g", "d", "h"),
      //      "ABCD" -> Array("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"),
      //      "A" -> Array("a+b+c+d+e+f+g+h", "i+j+k+l+m+n+o+p"),
      //      "B" -> Array("a+b+c+d+i+j+k+l", "e+f+g+h+m+n+o+p"),
      //      "C" -> Array("a+b+e+f+i+j+m+n", "c+d+g+h+k+l+o+p"),
      //      "D" -> Array("a+c+e+g+i+k+m+o", "b+d+f+h+j+l+n+p"),
      //      "AB" -> Array("a+b+c+d", "e+f+g+h", "i+j+k+l", "m+n+o+p"),
      //      "BA" -> Array("a+b+c+d", "i+j+k+l", "e+f+g+h", "m+n+o+p"),
      //      "BD" -> Array("a+c+i+k", "b+d+j+l", "e+g+m+o", "f+h+n+p"),
      //      "CD" -> Array("a+e+i+m", "b+f+j+n", "c+g+k+o", "d+h+l+p"),
      //      "ABC" -> Array("a+b", "c+d", "e+f", "g+h", "i+j", "k+l", "m+n", "o+p"),
      //      "BAC" -> Array("a+b", "c+d", "i+j", "k+l", "e+f", "g+h", "m+n", "o+p"),
      //      "BCA" -> Array("a+b", "i+j", "c+d", "k+l", "e+f", "m+n", "g+h", "o+p"),
      //      "ABD" -> Array("a+c", "b+d", "e+g", "f+h", "i+k", "j+l", "m+o", "n+p"),
      //      "ACD" -> Array("a+e", "b+f", "c+g", "d+h", "i+m", "j+n", "k+o", "l+p"),
      //      "BCD" -> Array("a+i", "b+j", "c+k", "d+l", "e+m", "f+n", "g+o", "h+p"),
    ).foreach {
      tuple =>
        println(s"${tuple._1} = [${
          t.forward(tuple._2).map {
            expr => expr.split("(?=[+|-])").sorted.mkString
          }.mkString(", ")
        }]")
    }


  }

  class OperationPrinter extends Fractional[String] with ClassTag[String] {
    override def minus(x: String, y: String): String = {
      plus(x, invert_signs(normalize(y)))
    }

    override def plus(x: String, y: String): String = {
      if (x.matches("[+-]?0")) {
        return y
      } else if (y.matches("[+-]?0")) {
        return x
      }

      val x_parts = x.split("/")
      val y_parts = y.split("/")

      if (x_parts.length == 1 && y_parts.length == 1) {
        return s"${normalize(x)}${normalize(y)}"
      } else if (x_parts.length == 2 && y_parts.length == 2 && x_parts(1) == y_parts(1)) {
        return s"${normalize(x_parts(0))}${normalize(y_parts(0))}/${x_parts(1)}"
      } else {
        println(s"plus($x, $y)")
        ???
      }
    }

    private def normalize(expr: String): String = {
      if (expr.matches("[a-z][a-z+-]*")) {
        s"+$expr"
      } else {
        expr
      }
    }

    private def invert_signs(s: String): String = {
      s.map {
        case '+' => '-'
        case '-' => '+'
        case c => c
      }
    }

    override def times(x: String, y: String): String = {
      if (x.matches("[+-]?0") || y.matches("[+-]?0")) {
        return "0"
      } else if (x.matches("[+]?1")) {
        return y
      } else if (y.matches("[+]?1")) {
        return x
      }

      val x_parts = x.split("/")

      if (x_parts.length == 1 && x.matches("[a-z+-]*") && y.matches("-1")) {
        return invert_signs(normalize(x))
      } else if (x_parts.length == 2 && x_parts(0).matches("[a-z+-]*") && y.matches("-1")) {
        return s"${invert_signs(normalize(x_parts(0)))}/${x_parts(1)}"
      } else {
        println(s"times($x, $y)")
        ???
      }

    }

    override def div(x: String, y: String): String = {
      val x_parts = x.split("/")

      if (x_parts.length == 1 && y.matches("[0-9]+")) {
        s"$x/$y"
      } else if (x_parts.length == 2 && x_parts(1).matches("[0-9]+") && y.matches("[0-9]+")) {
        s"${x_parts(0)}/${parseInt(x_parts(1)) * parseInt(y)}"
      } else {
        println(s"div($x, $y)")
        ???
      }
    }

    override def compare(x: String, y: String): Int = x.compareTo(y)

    override def fromInt(x: Int): String = x.toString

    override def negate(x: String): String = ???

    override def toInt(x: String): Int = ???

    override def toLong(x: String): Long = ???

    override def toFloat(x: String): Float = ???

    override def toDouble(x: String): Double = ???

    override def runtimeClass: Class[String] = classOf[String]
  }
}
