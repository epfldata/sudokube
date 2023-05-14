package core.solver.wavelet

import jwave.Transform
import jwave.transforms.FastWaveletTransform
import jwave.transforms.wavelets.haar.Haar1

import scala.math.Fractional.Implicits.infixFractionalOps
import scala.reflect.ClassTag

abstract class Transformer[T](val name: String, implicit val T: Fractional[T]) {
  def forward(array: Array[T]): Array[T]

  def reverse(wavelet: Array[T]): Array[T]
}

class HaarTransformer[T](implicit fractional: Fractional[T], classTag: ClassTag[T]) extends Transformer[T]("Haar", fractional) {
  override def forward(array: Array[T]): Array[T] = {
    /*
    2×2 Haar matrix that is associated with the Haar wavelet is H_2 = [[1, 1], [1, -1]].

    The 2N×2N Haar matrix H_2N is obtained by the following procedure: H_2N = [H_N ⊗ [1, 1], I_N ⊗ [1, -1]],
    where ⊗ denotes the Kronecker product.
    */
    val wavelet: Array[T] = array.clone()

    val num_of_transform_steps = Math.log(array.length) / Math.log(2)

    var l = 0
    var h = wavelet.length
    while (h >= 2 && l < num_of_transform_steps) {

      val smaller_wavelet = forward_partial(wavelet, h)
      System.arraycopy(smaller_wavelet, 0, wavelet, 0, h)

      h = h >> 1
      l += 1
    } // levels

    return wavelet;
  }

  private def forward_partial(array: Array[T], wavelet_length: Int): Array[T] = {
    val wavelet = new Array[T](wavelet_length)

    val h = wavelet.length >> 1 // .. -> 8 -> 4 -> 2 .. shrinks in each step by half wavelength

    for (i <- 0 until h) {
      wavelet(i) = fractional.zero
      wavelet(i + h) = fractional.zero

      for (j <- 0 until 2) {
        // k = ( i * 2 ) + j
        var k = (i << 1) + j

        while (k >= wavelet.length) k -= wavelet.length // circulate over arrays if scaling and wavelet are are larger

        wavelet(i) += array(k) // / fractional.fromInt(2)
        wavelet(i + h) += array(k) * (if (j == 0) fractional.fromInt(1) else fractional.fromInt(-1)) // / fractional.fromInt(2)
      }
    }

    return wavelet
  }

  override def reverse(wavelet: Array[T]): Array[T] = {
    val array = wavelet.clone()

    var h = 2
    while (h <= array.length && h >= 2) {

      val smaller_array = reverse_partial(array, h)
      System.arraycopy(smaller_array, 0, array, 0, h)

      h = h << 1
    }

    return array
  }

  private def reverse_partial(wavelet: Array[T], array_length: Int): Array[T] = {
    val array = Array.fill(array_length)(fractional.zero)

    val h = array.length >> 1 // .. -> 8 -> 4 -> 2 .. shrinks in each step by half wavelength

    for (i <- 0 until h) {
      // a[2i] = (total + detail_coefficient) / 2
      // a[2i + 1] = (total - detail_coefficient) / 2
      for (j <- 0 until 2) {
        var k = (i << 1) + j

        while (k >= array.length) k -= array.length // circulate over arrays if scaling and wavelet are are larger

        array(k) += (wavelet(i) + wavelet(i + h) * (if (j == 0) fractional.fromInt(1) else fractional.fromInt(-1))) / fractional.fromInt(2)
      }
    }

    return array
  }

}

class JWaveHaarTransformer extends Transformer[Double](name = "JWaveHaar", T = implicitly[Fractional[Double]]) {

  val transformer = new Transform(new FastWaveletTransform(new Haar1()))

  override def forward(array: Array[Double]): Array[Double] = transformer.forward(array)

  override def reverse(array: Array[Double]): Array[Double] = transformer.reverse(array)
}

class HaarKroneckerTransformer[T](implicit fractional: Fractional[T]) extends Transformer[T]("Haar-Kronecker", fractional) {

  override def forward(array: Array[T]): Array[T] = {
    val wavelet = array.clone()

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
          array(j) = (a + b) / fractional.fromInt(2)
          array(j + h) = (a - b) / fractional.fromInt(2)

          j += 1
        }
        i += h * 2
      }
      h /= 2
    }

    return array
  }
}
