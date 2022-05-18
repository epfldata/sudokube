import frontend.{AND, ArrayFunctions, EXIST, FORALL, MOMENT, NUM_ROWS, OR, TUPLES_PREFIX, UserCube, VALUES_ROWS}
import org.scalatest.{FlatSpec, Matchers}

class ArrayFunctionsSpec extends FlatSpec with Matchers{
  def fixture = new {
    val userCube = UserCube.createFromJson("recipes.json", "rating")
  }

  it should "find value of prefix correctly" in {
    assert(ArrayFunctions.findValueOfPrefix("India=x1;Computer=y2", "Computer", false) == "y2")
    info("works for single value, returns only one")
    assert(ArrayFunctions.findValueOfPrefix("India=x1;Computer=y2", "Computer", true) == "y2")
    info("works for single value, returns multiple")
    assert(ArrayFunctions.findValueOfPrefix("India=x1;Computer=(y2, y3)", "Computer", false) == "y2")
    info("works when multiple values, returns only one")
    assert(ArrayFunctions.findValueOfPrefix("India=x1;Computer=(y2, y3)", "Computer", true) == "(y2, y3)")
    info("works when multiple values, return multiple")
    assert(ArrayFunctions.findValueOfPrefix("India=x1;Computer=y2", "undefined_value", false) == " ")
    info("returns empy when undefned prefix")
  }

  it should "perform window_aggregates" in {
    val res = fixture.userCube.query(List(("Region", 2, Nil), ("difficulty", 6, Nil)), Nil, OR, MOMENT, TUPLES_PREFIX)
      .asInstanceOf[Array[Any]]
    var result = ArrayFunctions.window_aggregate(res, "difficulty", 3, NUM_ROWS)
    assert(result(1)._2 == result(2)._2 && result(2)._2 == result(3)._2)
    info("window based on number of rows works")
    result = ArrayFunctions.window_aggregate(res, "difficulty", 3, VALUES_ROWS)
    assert(result(8)._2 == 12.0 && result(0)._2 == 24.0)
    info("window based on value of rows works")
  }

  it should "apply binary function to query" in {
    var array = fixture.userCube.query(List(("Region", 2, List("India", "Italy")), ("spicy", 2, List(">=0"))), Nil, AND, MOMENT, TUPLES_PREFIX)
      .asInstanceOf[Array[Any]].map(x => x.asInstanceOf[(String, Any)])
    assert(ArrayFunctions.applyBinary(array, binaryFunction, ("Region", "spicy"), EXIST))
    assert(!ArrayFunctions.applyBinary(array, binaryFunction, ("Region", "spicy"), FORALL))
    assert(!ArrayFunctions.applyBinary(array, binaryFunction, ("not_defined", "spicy"), EXIST))
    array = fixture.userCube.query(List(("Region", 2, List("India")), ("spicy", 2, List(">0"))), Nil, AND, MOMENT, TUPLES_PREFIX)
      .asInstanceOf[Array[Any]].map(x => x.asInstanceOf[(String, Any)])
    assert(ArrayFunctions.applyBinary(array, binaryFunction, ("Region", "spicy"), EXIST))
    assert(ArrayFunctions.applyBinary(array, binaryFunction, ("Region", "spicy"), FORALL))

    def binaryFunction(str1: Any, str2: Any): Boolean = {
      str1.toString.equals("India") && str2.toString == "1"
    }
  }
}
