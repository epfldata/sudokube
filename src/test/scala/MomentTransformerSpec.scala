import core.solver.moment.{Moment0Transformer, Moment1Transformer}
import core.solver.{Rational, RationalTools}
import org.scalatest.{FlatSpec, Matchers}

class MomentTransformerSpec extends FlatSpec with Matchers {
  val actual = Array(0, 1, 3, 1, 7, 2, 3, 0).map(Rational(_, 1))
  val moment0 = Array(17, 13, 10, 7, 5, 3, 1, 0).map(Rational(_, 1))
  val comoment1 = Array(Rational(17, 1), Rational(0, 1), Rational(0, 1), Rational(-11, 17), Rational(0, 1), Rational(-14, 17), Rational(-33, 17), Rational(26, 289))

  val moment1 = Array(17, 4, 7, 1, 12, 2, 3, 0).map(Rational(_, 1))
  val momentProduct1 = moment1.map(x => x / moment1(0))
  implicit val num = RationalTools.RationalOps
  "Moment1 Transformer Forward " should "be correct" in {
    val result = Moment1Transformer().getMoments(actual)
    assert(result.sameElements(moment1))
  }

  "Moment1 Transformer Reverse " should "be correct" in {
    val result = Moment1Transformer().getValues(moment1)
    assert(result.sameElements(actual))
  }

  "Moment1 Transformer Complement " should "be correct" in {
    val result = Moment1Transformer().fromComplementaryMoment(moment0)
    assert(result.sameElements(moment1))
  }

  "Moment1 Transformer CoMoment" should "be correct" in {
    val result = Moment1Transformer().getCoMoments(actual, (0 until 3).map(i => momentProduct1(1 << i)).toArray)
    assert(result.sameElements(comoment1))
  }

  "Moment0 Transformer Forward " should "be correct" in {
    val result = Moment0Transformer().getMoments(actual)
    assert(result.sameElements(moment0))
  }

  "Moment0 Transformer Reverse " should "be correct" in {
    val result = Moment0Transformer().getValues(moment0)
    assert(result.sameElements(actual))
  }

  "Moment0 Transformer Complement " should "be correct" in {
    val result = Moment0Transformer().fromComplementaryMoment(moment1)
    assert(result.sameElements(moment0))
  }

}
