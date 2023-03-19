package core.solver.wavelet

import jwave.Transform
import jwave.transforms.FastWaveletTransform
import jwave.transforms.wavelets.haar.Haar1

import scala.math.Fractional.Implicits.infixFractionalOps

abstract class Transformer[T](val name: String, implicit val T: Fractional[T]) {
  def forward(array: Array[T]): Array[T]

  def reverse(array: Array[T]): Array[T]
}

class HaarTransformer[T: Fractional] extends Transformer[T](name = "Haar", T = implicitly[Fractional[T]]) {


  override def forward(array: Array[T]): Array[T] = {
    val wavelet = array.clone()
    println(s"arr: ${array.mkString(",")}")
    println("----")

    /* Kronecker product with matrix [[1, 1], [1, -1]] */
    var h = 1
    while (h < array.length) {
      var i = 0
      while (i < array.length) {
        var j = i
        while (j < i + h) {
          val a = wavelet(j)
          val b = wavelet(j + h)
          wavelet(j) = a + b
          wavelet(j + h) = a - b

          println(s"h: $h, i: $i, j: $j")
          println(s"[$j]: $a, [${j + h}]: $b  -> [$j] = $a + $b, [${j + h}] = $a - $b")
          println(s"arr: ${wavelet.mkString(",")}")
          println("----")

          j += 1
        }
        i += h * 2
      }
      h *= 2
    }

    return wavelet
  }

  override def reverse(wavelet: Array[T]): Array[T] = {
    val array = wavelet.clone()

    var h = array.length / 2
    while (h > 0) {
      var i = 0
      while (i < array.length) {
        var j = i
        while (j < i + h) {
          val a = array(j)
          val b = array(j + h)
          array(j) = (a + b) / T.fromInt(2)
          array(j + h) = (a - b) / T.fromInt(2)

          j += 1
        }
        i += h * 2
      }
      h /= 2
    }

    return array
  }
}

class JWaveHaarTransformer extends Transformer[Double](name = "JWaveHaar", T = implicitly[Fractional[Double]]) {

  val transformer = new Transform(new FastWaveletTransform(new Haar1()))

  override def forward(array: Array[Double]): Array[Double] = transformer.forward(array)

  override def reverse(array: Array[Double]): Array[Double] = transformer.reverse(array)
}