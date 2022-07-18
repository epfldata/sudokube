package frontend


case object TestLine {
  /**
   * transforms the data to facilitate slice operation, and call the correct function according to the op operator
   * @param op operator between the dimensions (for example a slice query can be on ((Region, 2, List(India, Italy)), (difficulty, 3, List(">4")))
   *           will be (Region = India || Region = Italy) op (difficulty > 4)
   * @param splitString header of the row value, with each different dimension being a value of the array
   * @param q_unsorted the slice query, with each of the form (prefix, List(slice_value_1, slice_value_2...))
   * @return true if the slice condition is not fulfilled, false otherwise
   */
  def testLineOp(op: OPERATOR, splitString: Array[String], q_unsorted: IndexedSeq[(String, List[String])]): Boolean = {
    if (q_unsorted.isEmpty) { //return false if the slice operation is empty
      return false
    }
    val newString = splitString.filter(x => q_unsorted.map(y => y._1).contains(x.split("=")(0))) //check if the splitString contains a field of interest for the slice operation
    testLine(newString.sortBy(_.toLowerCase), q_unsorted.sortBy(_._1.toLowerCase), 0, op)
  }

  /**
   * recursively checks if the provided string checks one of the criteria of qV_sorted
   * you can prefix >, <, >=, <= or ! (not equal) to compare more easily
   * @param splitString string to test
   * @param q_sorted    criteria, in form (field, list of acceptable values)
   * @param n           index of field considered
   * @return true <=> all the conditions are not fulfilled
   */
  private def testLine(splitString: Array[String], q_sorted: IndexedSeq[(String, List[String])], n: Int, op : OPERATOR): Boolean = {
    if (q_sorted.flatMap(x => x._2).isEmpty) {
      return false
    }
    if (n != splitString.length) {
      if (q_sorted(n)._2.isEmpty) {
        return testLine(splitString, q_sorted, n + 1, op)
      }
      for (i <- q_sorted(n)._2.indices) {
        q_sorted(n)._2(i) match {
          case s if s.startsWith("!") =>
            //when specified that the string must NOT equal a value
            return checkCondition(1, splitString(n), s, _ != _, testLine(splitString, q_sorted, n + 1, op), op)
          case s if s.startsWith(">=") =>
            return checkCondition(2, splitString(n), s, _ >= _, testLine(splitString, q_sorted, n + 1, op), op)
          case s if s.startsWith("<=") =>
            return checkCondition(2, splitString(n), s, _ <= _, testLine(splitString, q_sorted, n + 1, op), op)
          case s if s.startsWith(">") =>
            return checkCondition(1, splitString(n), s, _ > _, testLine(splitString, q_sorted, n + 1, op), op)

          case s if s.startsWith("<") =>
            return checkCondition(1, splitString(n), s, _ < _, testLine(splitString, q_sorted, n + 1, op), op)
          case s =>
            return checkCondition(0, splitString(n), s, _ == _, testLine(splitString, q_sorted, n + 1, op), op)
        }
      }
    }
    op match {
      case AND => false
      case OR => true
    }
  }

  /**
   * test if a condition is satisfied on a value
   * @param offset the size of the comparison prefix to discard from the condition string
   * @param value the value, in form prefix=... or prefix=(val1, val2, ...)
   * @param condition the condition, with a prefix of size offset for comparison
   * @param operation the operation to execute (equality, comparison...)
   * @param callback the next function to execute if we need it, here the next condition check
   * @param op the operator, AND or OR
   * @return
   */
  private def checkCondition(offset: Int, value: String, condition: String, operation: (Float, Float) => Boolean, callback:  => Boolean, op: OPERATOR): Boolean = {
    try {
      //retrieve all the numbers in this string, and test if one of them matches the criterion
      val cond = condition.substring(offset).toFloat
      if (("""\d+""".r findAllIn value).toList.exists(string => operation(string.toFloat, cond))) {
        return op match {
          case AND => callback
          case OR => false
        }
      }
    } catch {
      case e : Exception => // if exception in toFloat conversion of cond, the value is not a number hence we simply compare the strings
        if (value.split("=")(1).replaceAll("[()]", "").split(", ").contains(condition)) {
          return op match {
            case AND => callback
            case OR => false
          }
        }
    }
    op match {
      case AND => true
      case OR => callback
    }
  }
}
