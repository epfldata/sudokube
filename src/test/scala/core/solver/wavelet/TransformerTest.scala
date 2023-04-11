package core.solver.wavelet

import org.scalatest.FunSuite
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.prop.TableFor1

import scala.util.Random

class TransformerTest extends FunSuite {

  val transformers: TableFor1[Transformer[Double]] = Table(
    "transformers",
    new HaarTransformer[Double]()
  )

  forAll(transformers) { transformer =>
    // test round-trip for each transformer

    // create array of doubles, of size 2^k for some random k
    val size = 1 << (1 + Random.nextInt(10))
    val array = Array.fill(size)(Random.nextDouble())

    // transform and reverse
    val wavelet = transformer.forward(array)
    val array_ = transformer.reverse(wavelet)

    // check that the result is the same as the original
    assert(array sameElements array_)
  }
}
