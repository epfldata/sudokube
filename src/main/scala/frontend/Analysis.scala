package frontend

import frontend.schema.TimeSeriesSchema

class Analysis(uc : UserCube, sch : TimeSeriesSchema) {

    var list : IndexedSeq[String] = sch.columnList.filter(x => x._1 != "Time").map(x => x._1)
    var initList : List[String] = Nil

    /**
     * analyse the dataset with the isNullSchema (TimeSeriesSchema)
     * 
     * @return a list of (Int, List[Possibility]), Int corresponds to the Time (Number of line) and the list corresponds of all possibilities of change can happens
     */
    def analyse() : List[(Int, List[List[Possibility]])] = {
        /**
         * Recursively function, its method divide (or zoom in the query) by 2 if only if in this part of query there is a change and this function analyse the change
         * 
         * @param i  : the number of bits of Time that we want consider
         * @param l  : the only value of Time to consider
         * @param qV : the other column we want consider
         * @return a list of (Int, List[Possibility]), Int corresponds to the Time (Number of line) and the list corresponds of all possibilities of change can happens
         */
        def divideSub(i : Int, l : List[String], qV : IndexedSeq[(String, Int, List[String])]) : List[(Int, List[List[Possibility]])] = {
            if(sch.columns("Time").bits.length == i){
                return Nil
            }

            val r : Array[Any] = uc.query(Vector(("Time", i, l)) ++ qV, IndexedSeq.empty, AND, MOMENT, resultForm = TUPLES_PREFIX) match {
                case x : Array[Any] => x
                case _ => Array[Any]()
            }

            val s : Array[(String, AnyVal)] = r.map(y => {
                y match {
                    case y : (String, AnyVal) => y
                    case _ => ("", 0L)
                }
            })
            val hasC = hasChange(s)
            if(hasC.length > 1){
                val split : (List[String], List[String]) = splitArray(l)
                val prev : Int = split._1(split._1.length - 1).toInt
                val next : Int = split._2(0).toInt
                val newQV : IndexedSeq[(String, Int, List[String])] = ignoreColumn(hasC, qV)
                list = newQV.map(x => x._1)
                val change : List[(Int, List[List[Possibility]])] = detectedChange(uc.query(Vector(("Time", sch.columns("Time").bits.toList.length, List(prev.toString))) ++ newQV, Vector(), AND, MOMENT, resultForm = TUPLES_PREFIX) match {
                case p : Array[Any] => p
                case _ => Array[Any]()
            }, uc.query(Vector(("Time", sch.columns("Time").bits.toList.length, List(next.toString))) ++ newQV, Vector(), AND, MOMENT, resultForm = TUPLES_PREFIX) match {
                case p : Array[Any] => p
                case _ => Array[Any]()
            }, prev, next)

                
                return change ++ divideSub(i+1, split._1, newQV) ++ divideSub(i+1, split._2, newQV)
            }else return Nil
        }
        val max : Int = (Math.pow(2, sch.columns("Time").bits.toList.length).toInt - 1)
        initList = Range.inclusive(0, max).toList.map(_.toString)
        val qV : IndexedSeq[(String, Int, List[String])] = list.map(x => (x, 1, Nil))
        return divideSub(0, Nil, qV).sortBy(_._1)
  }

  /**
   * Show all the possibilities of change by line
   * 
   * @param y : List of all the possibilities of change by line
   */
  def show(y : List[(Int, List[List[Possibility]])]) : Unit = {
    if(y == Nil || y.isEmpty){
        println("There is no change")
    }else {
    y.foreach(w => {
            println("Line : " + w._1)
            w._2.foreach(p => {
            println("Possibilities of ")
            p.foreach(q => {
                println(q + "   ")
            })
            println()
            })
            })
    }
  }

  /**
   * 
   * Split the list of value into 2 lists
   * @param array : list of the value
   * @return (list, list) vector that contains 2 lists that represent the splitting of the original list
   */
  private def splitArray(array : List[String]) : (List[String], List[String]) = {
      if(array == Nil || array.isEmpty)
        return splitArray(initList)
      val limit = array.length/2 - 1
      val init : List[Int] = array.map(_.toInt).sorted
      var r1 : List[String] = Nil
      var r2 : List[String] = Nil
      for(i <- 0 to limit) {
          r1 = r1 ++ List(init(i).toString)
          r2 = r2 ++ List(init(limit+i+1).toString)
      }
      return (r1, r2)
  }

  /**
   * Filter the result of the query to obtain only the schema that is present in the part of dataset (in the part of the query)
   * 
   * @param array : result of the query
   * @return the filter result of the query
   */
  private def hasChange(array : Array[(String, AnyVal)]) : Array[String] = {
      val inter : Array[(String,String)] = array.map(x => (x._1, x._2.toString))
      val inter2 : Array[String] = inter.map(x=> (x._1, x._2.toDouble)).filter(y => y._2!=0).map(x=> x._1)
      return inter2
  }

  /**
   * 
   * Filter the list of column to consider
   * 
   * @param query : filter result query
   * @param qV : list of the column to filter
   * @return list of column to consider (in the list we have only the column changing in the filter query)
   */
  private def ignoreColumn(query : Array[String], qV : IndexedSeq[(String, Int, List[String])]) : IndexedSeq[(String, Int, List[String])] = {
    if(qV.isEmpty){
        return IndexedSeq.empty
    }
    val inter : Array[String] = query.filter(_.contains(qV.head._1 + "=NULL"))
    if(inter.length == query.length){
        return ignoreColumn(query, qV.tail)
    }
    //TODO: Converted List to Vector naively. Check if can be added at the end
    return Vector(qV.head) ++ ignoreColumn(query, qV.tail)
  }

  /**
   * 
   * Detected change between 2 lines
   * @param prev : query on only one line
   * @param next : query on only one line (line after prev)
   * @param numberPrev : number of line prev
   * @param numberNext : number of line next
   * @return list of all possibilities of change for this lines
   */
  def detectedChange(prev : Array[Any], next : Array[Any], numberPrev : Int, numberNext : Int) : List[(Int, List[List[Possibility]])] = {
    val pr : Array[String] = prev.map(x => x match {
            case y : (String, AnyVal) => y
            case _ => ("", 0L)
      }).filter(x => x._2.toString.toDouble!=0).map(x=> x._1)
      val ne : Array[String] = next.map(x => x match {
            case y : (String, AnyVal) => y
            case _ => ("", 0L)
      }).filter(x => x._2.toString.toDouble!=0).map(x=> x._1)
      if(pr.isEmpty || ne.isEmpty || pr(0).split(";Time=")(0) == ne(0).split(";Time=")(0)) {
          return Nil
      }
      var listAna : List[List[Possibility]] = Nil
      for(c <- list) {
          val valuePrev : String = ArrayFunctions.findValueOfPrefix(pr(0), c, false)
          val valueNext : String = ArrayFunctions.findValueOfPrefix(ne(0), c, false)
          if(valuePrev != valueNext) {
              var tempList : List[List[Possibility]] = Nil
            if(listAna.isEmpty) {
              if(valueNext == "NULL") {
                  listAna = List(List(new Remove(c)))
              }else {
                    listAna = List(List(new Add(c)))
              }
          }else {
          for(l <- listAna){
              if(valueNext == "NULL") {
                  tempList = tempList ++ analyseChangeToNull(Nil, l, c, Nil)
              }else {
                    tempList = tempList ++ analyseChangeToNotNull(Nil, l, c, Nil)
              }
          }
          listAna = tempList
          }
          }
      }
      return List((numberNext, listAna))
  }

  /**
   * Recursively analyse when a column value switch NOT_NULL to NULL
   * @param tempList : temporary list of all possibilities
   * @param l : list of possiblities to compare when we add the information that the column value passed of NOT_NULL to NULL
   * @param column : column to consider
   * @param already : the part of list of possibilities that we have already compared
   * @return list of all the possibilities when a column value switch NOT_NULL to NULL
   */
  private def analyseChangeToNull(tempList : List[List[Possibility]], l : List[Possibility], column : String, already : List[Possibility]) : List[List[Possibility]] = {
      if(l.isEmpty) {
          return tempList ++ List(already ++ List(new Remove(column)))
      }
      var list = tempList
      l.head match {
          case Add(x) => {
              list = list ++ List(already ++ List(new Rename(column, x)) ++ l.tail)
          }
          case Rename(x, y) => {
              list = list ++ List(already ++ List(new Rename(column, y), new Remove(x)) ++ l.tail)
          }
          case _ => list = list
      }
      return analyseChangeToNull(list, l.tail, column, already ++ List(l.head))
  }

  /**
   * Recursively analyse when a column value switch NULL to NOT_NULL
   * @param tempList : temporary list of all possibilities
   * @param l : list of possiblities to compare when we add the information that the column value passed of NULL to NOT_NULL
   * @param column : column to consider
   * @param already : the part of list of possibilities that we have already compared
   * @return list of all the possibilities when a column value switch NULL to NOT_NULL
   */
  private def analyseChangeToNotNull(tempList : List[List[Possibility]], l : List[Possibility], column : String, already : List[Possibility]) : List[List[Possibility]] = {
      if(l.isEmpty) {
          return tempList ++ List(already ++ List(new Add(column)))
      }
      var list = tempList
      l.head match {
          case Remove(x) => {
              list = list ++ List(already ++ List(new Rename(x, column)) ++ l.tail)
          }
          case Rename(x, y) => {
              list = list ++ List(already ++ List(new Rename(x, column), new Add(y)) ++ l.tail)
          }
          case _ => list = list
      }
      return analyseChangeToNotNull(list, l.tail, column, already ++ List(l.head))
  }
}

abstract class Possibility {
    override def toString() : String = {
        return "Possibilities"
    }
}
/**
 * Possibility to add a column
 * @param x : column to add
 */
case class Add(x : String) extends Possibility {
    override def toString() : String = {
        return "Add the column " + x
    }
}
/**
 * Possibility to rename a column
 * @param x : name of the column to rename
 * @param y : new name of this column
 */
case class Rename(x : String, y : String) extends Possibility{
    override def toString() : String = {
        return "Rename the column " + x + " to the column " + y
    }
}
/**
 * Possibility to remove a column
 * @param x : column to remove
 */
case class Remove(x : String) extends Possibility {
    override def toString() : String = {
        return "Remove the column " + x
    }
}

