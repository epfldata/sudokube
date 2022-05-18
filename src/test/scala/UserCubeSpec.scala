import breeze.linalg.DenseMatrix
import frontend._
import org.scalatest.{FlatSpec, Matchers}

class UserCubeSpec extends FlatSpec with Matchers{
  def fixture = new {
    val userCube = UserCube.createFromJson("recipes.json", "rating")
  }

  it should "return right bits for cosmetic dimensions" in {
    val userCube = fixture.userCube
    assert(List(6) == userCube.accCorrespondingBits("Type", 1, 0, Nil))
    assert(List(6, 13) == userCube.accCorrespondingBits("Type", userCube.sch
      .n_bits, 0, Nil))
    assert(List() == userCube.accCorrespondingBits("not a field", userCube.sch.n_bits, 0, Nil))
  }

  it should "return good matrix(not sliced)" in {
    val userCube = fixture.userCube
    val matrix = userCube.query(List(("Region", 2, Nil), ("spicy", 1, Nil)),List(("Vegetarian", 1, Nil)), AND, MOMENT, MATRIX).asInstanceOf[DenseMatrix[String]]
    assert("15.0" == matrix(3, 1))
    assert("12.0" == matrix(4, 1))
    assert("5.0" == matrix(6, 2))
  }

  it should "sort result matrix by order of parameters" in {
    val userCube = fixture.userCube
    var matrix = userCube.query(List(("spicy", 1, Nil), ("Region", 2, Nil)),List(("Vegetarian", 1, Nil)), AND, MOMENT, MATRIX).asInstanceOf[DenseMatrix[String]]
    assert("15.0" == matrix(5, 1))
    assert("12.0" == matrix(7, 1))
    assert("5.0" == matrix(4, 2))
    matrix = userCube.query(List(("Vegetarian", 1, Nil)),List(("spicy", 1, Nil), ("Region", 2, Nil)), AND, MOMENT, MATRIX).asInstanceOf[DenseMatrix[String]]
    assert("15.0" == matrix(1, 5))
    assert("12.0" == matrix(1, 7))
    assert("5.0" == matrix(2, 4))
  }

  it should "return same result in naive or moment method" in {
    val userCube = fixture.userCube
    val matrix1 = userCube.query(List(("spicy", 1, Nil), ("Region", 2, Nil)),List(("Vegetarian", 1, Nil)), AND, MOMENT, MATRIX)
    val matrix2 = userCube.query(List(("spicy", 1, Nil), ("Region", 2, Nil)),List(("Vegetarian", 1, Nil)), AND, NAIVE, MATRIX)
    assert(matrix1.equals(matrix2))
  }

  it should "be able to dice some rows" in {
    val userCube = fixture.userCube
    var matrix = userCube.query(List(("Region", 3, List("India")), ("spicy", 1, Nil),
      ("Type", 1, Nil)),List(("Vegetarian", 1, List("1", "NULL"))), AND, MOMENT, MATRIX).asInstanceOf[DenseMatrix[String]]
    assert(matrix.rows == 5)
    assert(matrix.cols == 2)
    matrix = userCube.query(List(("Region", 3, List("India")), ("spicy", 1, Nil),
      ("Type", 1, Nil)),List(("Vegetarian", 1, List("NoneValue"))), AND, MOMENT, MATRIX).asInstanceOf[DenseMatrix[String]]
    assert(matrix.rows == 1 && matrix.cols == 1)
  }

  it should "be able to load and save cubes" in {
    val userCube = fixture.userCube
    userCube.save("test")
    val loaded = UserCube.load("test")
    assert(loaded.cube.naive_eval(List(0)).sameElements(userCube.cube.naive_eval(List(0))))
    assert(loaded.sch.n_bits == userCube.sch.n_bits)
  }

  it should "work for array query" in {
    val userCube = fixture.userCube
    val result = userCube.query(List(("Region", 3, List("India")), ("spicy", 1,
      Nil),
      ("Type", 1, Nil)), List(("Vegetarian", 1, List("1", "NULL"))), AND,
      MOMENT, ARRAY)
    result match {
      case (top, left, values) =>
        assert(values.asInstanceOf[Array[Any]].map(y => y.toString) sameElements Array("0.0", "0.0", "0.0", "4.0"))
    }
  }

  it should "work for TuplesBits query" in {
    val userCube = fixture.userCube
    val result = userCube.query(List(("Region", 3, List("India")), ("spicy", 1, Nil),
      ("Type", 1, Nil), ("Vegetarian", 1, List("1", "NULL"))), Nil, AND, MOMENT, TUPLES_BIT)
    assert(result.asInstanceOf[Array[Any]].map(y => y.asInstanceOf[(String, Any)]._2) sameElements Array("0.0", "0.0",
      "0.0", "4.0"))
    assert(result.asInstanceOf[Array[Any]].map(y => y.asInstanceOf[(List[String], Any)]._1).apply(0)  == List("b4=0", "b5=0", "b6=0", "b9=0", "b10=1"))
  }

  it should "work for TuplesPrefix query" in {
    val userCube = fixture.userCube
    val result = userCube.query(List(("Region", 3, List("India")), ("spicy", 1, Nil),
      ("Type", 1, Nil), ("Vegetarian", 1, List("1", "NULL"))), Nil, AND, MOMENT, TUPLES_PREFIX)
    assert(result.asInstanceOf[Array[Any]].map(y => y.asInstanceOf[(String, Any)]._2) sameElements Array("0.0", "0.0",
      "0.0", "4.0"))
    assert(result.asInstanceOf[Array[Any]].map(y => y.asInstanceOf[(String, Any)]._1).apply(0)  == "spicy=0;Region=India;Type=(Dessert, NULL);Vegetarian=(1, " +
      "NULL)")
  }

  it should "work for querying a dimension" in {
    val userCube = fixture.userCube
    var result = userCube.queryDimension(("name", 2), "difficulty", MOMENT).asInstanceOf[Map[String, Any]]
    assert(result.values sameElements  List(10.0, 15.0, 10.0, 3.0))
    assert(result.keys sameElements Set("name=(Healthy Chicken Korma, Truffes au chocolat)", "name=(NULL, Panna Cotta)", "name=(Black Chana Masala, Mousse " +
      "au chocolat)", "name=(Biscuits de Noel aux amandes, Tagliatelles à la citrouille)"))
    result = userCube.queryDimension(("name", 2), null, MOMENT).asInstanceOf[Map[String, Any]]
    assert(result.values sameElements  List(8.0, 7.0, 8.0, 17.0))
    assert(result.keys sameElements Set("name=(Healthy Chicken Korma, Truffes au chocolat)", "name=(NULL, Panna Cotta)", "name=(Black Chana Masala, Mousse " +
      "au chocolat)", "name=(Biscuits de Noel aux amandes, Tagliatelles à la citrouille)"))
  }

  it should "work for aggregating and slicing on different values" in {
    val userCube = fixture.userCube
    val result = userCube.aggregateAndSlice(List(("spicy", 1), ("Type", 2)), List(("Region", List("India")), ("Vegetarian", List(">=0"))), AND, MOMENT).map(x => x.asInstanceOf[(String, Any)])
    assert(result.apply(0)._1 ==  "spicy=0;Region=India;Type=NULL;Vegetarian=0" && result.apply(0)._2 == "0.0")
    assert(result.apply(3)._1 == "spicy=1;Region=India;Type=Dish;Vegetarian=0" &&  result.apply(3)._2 == "5.0")
  }

}
