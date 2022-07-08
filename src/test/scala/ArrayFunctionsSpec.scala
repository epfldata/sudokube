import frontend._
import org.scalatest.{FlatSpec, Matchers}

class ArrayFunctionsSpec extends FlatSpec with Matchers{
  def fixture = new {
    val userCube = UserCube.createFromJson("example-data/testing_database.json", "rating")
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
    info("returns empty when undefined prefix")
  }

  it should "perform window_aggregates" in {
    val res = fixture.userCube.query(List(("Region", 2, Nil), ("difficulty", 6, Nil)), Nil, OR, MOMENT, TUPLES_PREFIX)
      .asInstanceOf[Array[Any]]
    var result = ArrayFunctions.windowAggregate(res, "difficulty", 3, NUM_ROWS)
    assert(result(1)._2 == result(2)._2 && result(2)._2 == result(3)._2)
    info("window based on number of rows works")
    result = ArrayFunctions.windowAggregate(res, "difficulty", 3, VALUES_ROWS)
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

  "testLine" should "works for and" in {
    var array = Array("spicy=45", "Region=(India, China)")
    var query = List(("Region", List("India")), ("spicy", Nil)) //select Region = India
    assert(!TestLine.testLineOp(AND, array, query))
    query = List(("Region", List("China")), ("spicy", Nil)) //select Region = China
    assert(!TestLine.testLineOp(AND, array, query))
    query = List(("Region", List("China", "India")), ("spicy", Nil)) //select Region = China || Region = China
    assert(!TestLine.testLineOp(AND, array, query))
    query = List(("Region", List("China", "India")), ("spicy", List("45"))) //select (Region = China || Region = India) && spicy = 45
    assert(!TestLine.testLineOp(AND, array, query))
    query = List(("Region", List("China", "India")), ("spicy", List("43"))) //select (Region = China || Region = India) && spicy = 43
    assert(TestLine.testLineOp(AND, array, query))
    query = List(("Region", List("Switzerland")), ("spicy", Nil)) //select Region = Switzerland
    assert(TestLine.testLineOp(AND, array, query))
    query = List(("Region", Nil), ("spicy", Nil)) //select All
    assert(!TestLine.testLineOp(AND, array, query))
    array = Array("spicy=45", "Region=(AmerIndia, China)")
    query = List(("Region", List("India")), ("spicy", Nil)) //select Region = India
    assert(TestLine.testLineOp(AND, array, query))
    info("works for simple equality")

    query = List(("Region", List("China", "!India")), ("spicy", List("45"))) //select (Region = China || Region != India) && spicy = 45
    assert(!TestLine.testLineOp(AND, array, query))
    query = List(("Region", List("!India")), ("spicy", List("45"))) //select Region != India && spicy = 45
    assert(TestLine.testLineOp(AND, array, query))
    query = List(("Region", List("!India")), ("spicy", List("!45"))) //select Region != India && spicy != 45
    assert(TestLine.testLineOp(AND, array, query))
    query = List(("Region", List("!India", "!China")), ("spicy", List("45"))) //select (Region != China || Region != India) && spicy = 45
    assert(TestLine.testLineOp(AND, array, query))
    info("works for not equality")

    array = Array("spicy=(42,45,67)", "Region=(India, China)")
    query = List(("Region", List("India")), ("spicy", List("<45"))) //select Region = India && spicy < 45
    assert(!TestLine.testLineOp(AND, array, query))
    query = List(("Region", List("India")), ("spicy", List("<42"))) //select Region = India && spicy < 42
    assert(TestLine.testLineOp(AND, array, query))
    query = List(("Region", List("India")), ("spicy", List(">42"))) //select Region = India && spicy > 42
    assert(!TestLine.testLineOp(AND, array, query))
    query = List(("Region", List("India")), ("spicy", List(">67"))) //select Region = India && spicy > 67
    assert(TestLine.testLineOp(AND, array, query))
    query = List(("Region", List("India")), ("spicy", List(">=67"))) //select Region = India && spicy >= 67
    assert(!TestLine.testLineOp(AND, array, query))
    query = List(("Region", List("India")), ("spicy", List(">=68"))) //select Region = India && spicy >= 68
    assert(TestLine.testLineOp(AND, array, query))
    query = List(("Region", List("India")), ("spicy", List("<=42"))) //select Region = India && spicy <= 42
    assert(!TestLine.testLineOp(AND, array, query))
    query = List(("Region", List("India")), ("spicy", List("<=41"))) //select Region = India && spicy <= 41
    assert(TestLine.testLineOp(AND, array, query))
    info("works for comparison")
  }

  "testLine" should "works for or" in {
    var array = Array("spicy=45", "Region=(India, China)")
    var query = List(("Region", List("India")), ("spicy", Nil)) //select Region = India
    assert(!TestLine.testLineOp(OR, array, query))
    query = List(("Region", List("China")), ("spicy", Nil)) //select Region = China
    assert(!TestLine.testLineOp(OR, array, query))
    query = List(("Region", List("China", "India")), ("spicy", Nil)) //select Region = China || Region = China
    assert(!TestLine.testLineOp(OR, array, query))
    query = List(("Region", List("China", "India")), ("spicy", List("43"))) //select (Region = China || Region = India) || spicy = 45
    assert(!TestLine.testLineOp(OR, array, query))
    query = List(("Region", List("Vietnam")), ("spicy", List("43"))) //select Region = Vietnam || spicy = 43
    assert(TestLine.testLineOp(OR, array, query))
    query = List(("Region", List("Vietnam")), ("spicy", List("45"))) //select Region = Vietnam || spicy = 43
    assert(!TestLine.testLineOp(OR, array, query))
    query = List(("Region", List("Switzerland")), ("spicy", Nil)) //select Region = Switzerland
    assert(TestLine.testLineOp(OR, array, query))
    query = List(("Region", Nil), ("spicy", Nil)) //select All
    assert(!TestLine.testLineOp(OR, array, query))
    info("works for simple equality")

    query = List(("Region", List("China", "!India")), ("spicy", List("45"))) //select (Region = China || Region != India) || spicy = 45
    assert(!TestLine.testLineOp(OR, array, query))
    query = List(("Region", List("!India")), ("spicy", List("45"))) //select Region != India || spicy = 45
    assert(!TestLine.testLineOp(OR, array, query))
    query = List(("Region", List("!India")), ("spicy", List("!45"))) //select Region != India || spicy != 45
    assert(TestLine.testLineOp(OR, array, query))
    query = List(("Region", List("!India", "!China")), ("spicy", List("45"))) //select (Region != China || Region != India) || spicy = 45
    assert(!TestLine.testLineOp(OR, array, query))
    info("works for not equality")

    array = Array("spicy=(42,45,67)", "Region=(India, China)")
    query = List(("Region", Nil), ("spicy", List("<45"))) //select spicy < 45
    assert(!TestLine.testLineOp(OR, array, query))
    query = List(("Region", Nil), ("spicy", List("<42"))) //select spicy < 42
    assert(TestLine.testLineOp(OR, array, query))
    query = List(("Region", Nil), ("spicy", List(">42"))) //select spicy > 42
    assert(!TestLine.testLineOp(OR, array, query))
    query = List(("Region", Nil), ("spicy", List(">67"))) //select spicy > 67
    assert(TestLine.testLineOp(OR, array, query))
    query = List(("Region", Nil), ("spicy", List(">=67"))) //select spicy >= 67
    assert(!TestLine.testLineOp(OR, array, query))
    query = List(("Region", Nil), ("spicy", List(">=68"))) //select spicy >= 68
    assert(TestLine.testLineOp(OR, array, query))
    query = List(("Region", Nil), ("spicy", List("<=42"))) //select spicy <= 42
    assert(!TestLine.testLineOp(OR, array, query))
    query = List(("Region", Nil), ("spicy", List("<=41"))) //select spicy <= 41
    assert(TestLine.testLineOp(OR, array, query))
    info("works for comparison")
  }

  "slope and intercept" should "find the correct slope and intercept" in {
    val userCube = fixture.userCube
    var res = userCube.queryDimension(("difficulty", 4, Nil), null, MOMENT)
    assert(ArrayFunctions.slopeAndIntercept(res.map(x => (x._1.toDouble, x._2))) == (-0.4420289855072464, 11.094202898550725))
    res = userCube.queryDimension(("spicy", 4, Nil), "difficulty", MOMENT)
    assert(ArrayFunctions.slopeAndIntercept(res.map(x => (x._1.toDouble, x._2))) == (-1.0, 18.0))
  }
}
