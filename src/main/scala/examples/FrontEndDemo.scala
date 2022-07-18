package examples

import breeze.linalg.DenseMatrix
import frontend._

/**
 * This a demo class to identify the functions working with UserCube and their respective utility
 */
object FrontEndDemo {
  /**
   * You can save an UserCube which will be stored in the 'cubeData' folder
   * It is stored in 2 files, one for the schema and one for the datacube itself, and can be loaded thereafter
   */
  def save_load(cube: UserCube): UserCube = {
    cube.save("demoCube")
    UserCube.load("demoCube")
  }

  /**
   * You can query for tuples, where each value in the matrix will have a header with the value of each of the dimension queried
   *  You can choose between the Tuples-bit (which will return the value of each of the bit queried in binary) and the Tuples-prefix (which will translate it to
   *  the real values)
   *  if using the tuples format you can use qV or qH indifferently
   */
  def query_tuples(userCube: UserCube) = {
    //can query for array of tuples with bit format
    //query for Type, 2 bits, and price, 2 bits
    var array = userCube.query(Vector(("Type", 2, Nil), ("price", 2, Nil)), Vector(), AND, MOMENT, TUPLES_BIT).asInstanceOf[Array[
      Any]].map(x => x.asInstanceOf[(String, Any)])
    println(array.mkString("(", "\n ", ")\n \n"))

    //can query for array of tuples with prefix format
    //query for Type, 2 bits, and price, 2 bits
    array = userCube.query(Vector(("Type", 2, Nil), ("price", 2, Nil)), Vector(), AND, MOMENT, TUPLES_PREFIX).asInstanceOf[Array[
      Any]].map(x => x.asInstanceOf[(String, Any)])
    println(array.mkString("(", "\n ", ")\n \n"))
  }

  /**
   * You can use the array query format, which will return an tuple containing the top header, the left header and the values of the matrix
   * As the query function return an Any object, it needs to be cast each and every time
   */
  def query_array(userCube: UserCube) = {
    //can query for an array, return top/left/values
    //query for Region, 2 bits (qV) and time, 2 bits (qH)
    val tuple = userCube.query(Vector(("Region", 2, Nil)), Vector(("time", 2, Nil)), AND, MOMENT, ARRAY).asInstanceOf[(Array[Any],
      Array[Any], Array[Any])]
    println(tuple._1.mkString("top header\n(", ", ", ")\n"))
    println(tuple._2.mkString("left header\n(", "\n ", ")\n"))
    println(tuple._3.mkString("values\n(", ", ", ")\n \n"))
  }


  /**
   * You can use the matrix query format, which will return a matrix with qV for the vertical header and qH for the horizontal.
   * You can specify the prefix name you want and the number of bits for it. You also have an internal sorting within
   * the headers in reverse order of the dimension prefixes given.
   */
  def query_matrix(userCube: UserCube) = {
    //query for price, 1 bit (qV) and time, 3 bits (qH)
    var matrix = userCube.query(Vector(("price", 1, Nil)), Vector(("time", 3, Nil)), AND, MOMENT, MATRIX).asInstanceOf[DenseMatrix[String]]
    println(matrix.toString(Int.MaxValue, Int.MaxValue) + "\n \n")

    //query for price, 1 bit, Region, 2 bits (qV) and time, 3 bits (qH)
    matrix = userCube.query(Vector(("Region", 2, Nil), ("price", 1, Nil)), Vector(("time", 3, Nil)), AND, MOMENT, MATRIX).asInstanceOf[DenseMatrix[String]]
    println(matrix.toString(Int.MaxValue, Int.MaxValue) + "\n \n")
  }

  /**
   * You can give more values to the query function to select some values accepted for a dimension
   * Note that you can choose the operator used BETWEEN the dimensions (AND or OR) but not within the dimensions (it will always be OR)
   * You can also prepend some comparison operators to the accepted values : ! (not equal) for all values and >, <, >= and <= only for numerical values
   * You also have a function that deletes the rows aggregating to zero - only for tuples format
   */
  def slice_and_dice(userCube: UserCube) = {
    //can slice and dice: select (Type = Dish || Type = Side) && price = cheap
    var array = userCube.query(Vector(("Type", 2, List("Dish", "Side")), ("price", 2, List("cheap"))), Vector(), AND, MOMENT, TUPLES_PREFIX).asInstanceOf[Array[
      Any]].map(x => x.asInstanceOf[(String, Any)])
    println(array.mkString("(", "\n ", ")\n \n"))

    //select (Type = Dish || Type = Side) || price = cheap
    array = userCube.query(Vector(("Type", 2, List("Dish", "Side")), ("price", 2, List("cheap"))), Vector(), OR, MOMENT, TUPLES_PREFIX).asInstanceOf[Array[
      Any]].map(x => x.asInstanceOf[(String, Any)])
    println(array.mkString("(", "\n ", ")\n \n"))
    //delete zero tuples
    array = ArrayFunctions.deleteZeroColumns(array)
    println(array.mkString("(", "\n ", ")\n \n"))
  }

  /**
   *
   * you can also apply a custom binary function to 2 dimensions, returning a boolean, and reduce using either FORALL (all rows must fulfill the prerequisite)
   * or EXIST (at least one row must fulfill the prerequisite)
   * Note that this 'applyBinary' function only works with Tuples prefix query format
   */
  def query_binary(userCube: UserCube) = {
    var array = userCube.query(Vector(("Type", 2, List("Dish", "Side")), ("price", 2, List("cheap"))), Vector(), OR, MOMENT, TUPLES_PREFIX).asInstanceOf[Array[
      Any]].map(x => x.asInstanceOf[(String, Any)])
    //delete zero tuples
    array = ArrayFunctions.deleteZeroColumns(array)
    //can apply some binary function, you simply specify in the tuple the 2 dimensions you want to test
    println(ArrayFunctions.applyBinary(array, binaryFunction, ("price", "Type"), EXIST))
    println(ArrayFunctions.applyBinary(array, binaryFunction, ("price", "Type"), FORALL) + "\n \n")

    def binaryFunction(str1: Any, str2: Any): Boolean = {
      str1.toString.equals("cheap") && !str2.toString.equals("Dish")
    }
  }

  /**
   * You can use the 'queryDimension' function to aggregate by a (numerical) dimension instead of the fact (in that example "difficulty")
   * You can also it to apply a custom groupBy function to your result
   * Note that this function evicts all the rows aggregating to 0
   */
  def query_dimension(userCube: UserCube) = {
    def transformForGroupBy(src : String): String = {
      src match {
        case "Europe" | "Italy" | "France" => "European"
        case _ => "Non-European"
      }
    }

    //can query another dimension, with TUPLES_PREFIX format
    println(userCube.queryDimension(("time", 4, Nil), "difficulty", MOMENT))
    //if the null is given to the aggregateDim, the normal fact is used
    println(userCube.queryDimension(("Region", 4, Nil), null, MOMENT) + "\n \n")
    println(userCube.queryDimension(("Region", 4, Nil), null, MOMENT, transformForGroupBy) + "\n \n")
  }

  /**
   * You can check if a dimension is monotonic or contains a double peak by calling the 'queryDimensionDoublePeak'
   * and 'queryDimensionMonotonic' function. You also provide a tolerance double to smooth the curve
   */
  def check_monotonicity_double_peak(userCube: UserCube) = {
    println(userCube.queryDimension(("time", 4, Nil), "difficulty", MOMENT))
    println(userCube.queryDimensionDoublePeak(("time", 4, Nil), "difficulty", MOMENT, 0.0))
    println(userCube.queryDimension(("difficulty", 4, Nil), null, MOMENT))
    println(userCube.queryDimensionDoublePeak(("difficulty", 4, Nil), null, MOMENT, 0.0))


    println(userCube.queryDimension(("time", 2, Nil), null, MOMENT))
    println(userCube.queryDimensionMonotonic(("time", 2, Nil), null, MOMENT, 1.0))
  }

  /**
   * computes the slope and intercept of a tuples prefix array
   */
  def check_slope_intercept(userCube: UserCube) = {
    var res = userCube.queryDimension(("difficulty", 4, Nil), null, MOMENT)
    println(ArrayFunctions.slopeAndIntercept(res.map(x => (x._1.toDouble, x._2))))

    res = userCube.queryDimension(("time", 4, Nil), "difficulty", MOMENT)
    println(ArrayFunctions.slopeAndIntercept(res.map(x => (x._1.toDouble, x._2))))
  }

  /**
   * you can compute a window aggregate, either by number of rows or by values of rows (this last can only be applied to numerical dimension)
   * Note that the gap specifies the size of the window
   */
  def check_aggregate(userCube: UserCube) = {
    val res = userCube.query(Vector(("Region", 1, Nil), ("time", 4, Nil)), Vector(), OR, MOMENT, TUPLES_PREFIX)
      .asInstanceOf[Array[Any]]

    var result = ArrayFunctions.windowAggregate(res, "time", 3, NUM_ROWS)
    println(result.mkString("\n\n", "\n ", "\n\n"))

    result = ArrayFunctions.windowAggregate(res, "time", 3, VALUES_ROWS)
    println(result.mkString("\n\n", "\n ", "\n\n"))
  }

  def main(args: Array[String]): Unit = {
    val cube = UserCube.createFromJson("example-data/demo_recipes.json", "rating") //create a UserCube from a csv or a json file
    val userCube = save_load(cube)

    query_matrix(userCube)

    query_array(userCube)

    query_tuples(userCube)

    slice_and_dice(userCube)

    query_binary(userCube)

    query_dimension((userCube))

    check_monotonicity_double_peak(userCube)

    check_slope_intercept(userCube)

    check_aggregate(userCube)
  }
}
