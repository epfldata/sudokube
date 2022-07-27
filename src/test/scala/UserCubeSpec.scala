import breeze.linalg.DenseMatrix
import frontend._
import org.scalatest.{FlatSpec, Matchers}

class UserCubeSpec extends FlatSpec with Matchers{
  def fixture = new {
    val userCube = UserCube.createFromJson("example-data/testing_database.json", "rating", "testUserCube")
  }

  it should "return right bits for cosmetic dimensions" in {
    val userCube = fixture.userCube
    assert(List(13) == userCube.accCorrespondingBits("Type", 1, Nil))
    assert(List(6, 13) == userCube.accCorrespondingBits("Type", userCube.sch
      .n_bits, Nil))
    assert(List() == userCube.accCorrespondingBits("not a field", userCube.sch.n_bits, Nil))
  }

  it should "return good matrix(not sliced)" in {
    val userCube = fixture.userCube
    val matrix = userCube.query(Vector(("Region", 2, Nil), ("spicy", 1, Nil)), Vector(("Vegetarian", 1, Nil)), AND, MOMENT, MATRIX).asInstanceOf[DenseMatrix[String]]
    assert("8.0" == matrix(3, 1))
    assert("9.0" == matrix(4, 1))
    assert("3.0" == matrix(4, 2))
  }

  it should "sort result matrix by order of parameters" in {
    val userCube = fixture.userCube
    var matrix = userCube.query(Vector(("spicy", 1, Nil), ("Region", 2, Nil)), Vector(("Vegetarian", 1, Nil)), AND, MOMENT, MATRIX).asInstanceOf[DenseMatrix[String]]
    assert("8.0" == matrix(5, 1))
    assert("9.0" == matrix(7, 1))
    assert("3.0" == matrix(7, 2))
    matrix = userCube.query(Vector(("Vegetarian", 1, Nil)), Vector(("spicy", 1, Nil), ("Region", 2, Nil)), AND, MOMENT, MATRIX).asInstanceOf[DenseMatrix[String]]
    assert("8.0" == matrix(1, 5))
    assert("9.0" == matrix(1, 7))
    assert("3.0" == matrix(2, 7))
  }

  it should "return same result in naive or moment method" in {
    val userCube = fixture.userCube
    val matrix1 = userCube.query(Vector(("spicy", 1, Nil), ("Region", 2, Nil)), Vector(("Vegetarian", 1, Nil)), AND, MOMENT, MATRIX)
    val matrix2 = userCube.query(Vector(("spicy", 1, Nil), ("Region", 2, Nil)), Vector(("Vegetarian", 1, Nil)), AND, NAIVE, MATRIX)
    assert(matrix1.equals(matrix2))
  }

  it should "be able to dice some rows" in {
    val userCube = fixture.userCube
    var matrix = userCube.query(Vector(("Region", 3, List("India")), ("spicy", 1, Nil),
      ("Type", 1, Nil)), Vector(("Vegetarian", 1, List("1"))), AND, MOMENT, MATRIX).asInstanceOf[DenseMatrix[String]]
    assert(matrix.rows == 5)
    assert(matrix.cols == 2)
    matrix = userCube.query(Vector(("Region", 3, List("India")), ("spicy", 1, Nil),
      ("Type", 1, Nil)), Vector(("Vegetarian", 1, List("NoneValue"))), AND, MOMENT, MATRIX).asInstanceOf[DenseMatrix[String]]
    assert(matrix.rows == 1 && matrix.cols == 1)
  }

  it should "be able to load and save cubes" in {
    val userCube = fixture.userCube
    userCube.save()
    val loaded = UserCube.load("testUserCube")
    assert(loaded.cube.naive_eval(Vector(0)).sameElements(userCube.cube.naive_eval(Vector(0))))
    assert(loaded.sch.n_bits == userCube.sch.n_bits)
  }

  it should "work for array query" in {
    val userCube = fixture.userCube
    val result = userCube.query(Vector(("Region", 3, List("India")), ("spicy", 1,
      Nil),
      ("Type", 1, Nil)), Vector(("Vegetarian", 1, List("NULL", "0"))), AND,
      MOMENT, ARRAY)
    result match {
      case (top, left, values) =>
        assert(values.asInstanceOf[Array[Any]].map(y => y.toString) sameElements Array("0.0", "9.0", "0.0", "0.0"))
    }
  }

  it should "work for TuplesBits query" in {
    val userCube = fixture.userCube
    val result = userCube.query(Vector(("Region", 3, List("India")), ("spicy", 1, Nil),
      ("Type", 1, Nil), ("Vegetarian", 1, List("0", "NULL"))), Vector(), AND, MOMENT, TUPLES_BIT)
    assert(result.asInstanceOf[Array[Any]].map(y => y.asInstanceOf[(String, Any)]._2) sameElements Array("0.0", "9.0",
      "0.0", "0.0"))
    assert(result.asInstanceOf[Array[Any]].map(y => y.asInstanceOf[(List[String], Any)]._1).apply(0)  == List("b4=0", "b5=0", "b10=0", "b12=0", "b13=1"))
  }

  it should "work for TuplesPrefix query" in {
    val userCube = fixture.userCube
    val result = userCube.query(Vector(("Region", 3, List("India")), ("spicy", 1, Nil),
      ("Type", 1, Nil), ("Vegetarian", 1, List("0", "NULL"))), Vector(), AND, MOMENT, TUPLES_PREFIX)
    assert(result.asInstanceOf[Array[Any]].map(y => y.asInstanceOf[(String, Any)]._2) sameElements Array("0.0", "9.0",
      "0.0", "0.0"))
    assert(result.asInstanceOf[Array[Any]].map(y => y.asInstanceOf[(String, Any)]._1).apply(0)  == "spicy=0;Region=India;Type=(Dish, NULL);Vegetarian=(0, " +
      "NULL)")
  }

  it should "work for querying a dimension" in {
    val userCube = fixture.userCube
    var result = userCube.queryDimension(("Region", 2, Nil), "difficulty", MOMENT).asInstanceOf[Vector[(String, Any)]]
    assert(result.map(x => x._2) sameElements  List(0.0, 17.0, 18.0, 3.0))
    assert(result.map(x => x._1) sameElements Set("Europe", "India", "Italy", "NULL"))
    result = userCube.queryDimension(("Region", 2, Nil), null, MOMENT).asInstanceOf[Vector[(String, Any)]]
    assert(result.map(x => x._2) sameElements  List(12.0, 9.0, 15.0, 4.0))
    assert(result.map(x => x._1) sameElements Set("Europe", "India", "Italy", "NULL"))
  }

  it should "work for aggregating and slicing on different values" in {
    val userCube = fixture.userCube
    val result = userCube.query(Vector(("spicy", 1, Nil), ("Type", 2, Nil), ("Region", userCube.sch.n_bits, List("India")), ("Vegetarian", userCube.sch.n_bits, List(">=0"))), Vector(), AND, MOMENT, TUPLES_PREFIX).asInstanceOf[Array[Any]].map(_.asInstanceOf[(String, Any)])
    assert(result.apply(0)._1 ==  "spicy=0;Region=India;Type=NULL;Vegetarian=0" && result.apply(0)._2 == "0.0")
    assert(result.apply(3)._1 == "spicy=1;Region=India;Type=Dish;Vegetarian=0" &&  result.apply(3)._2 == "5.0")
  }

}
