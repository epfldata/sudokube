package frontend

import frontend.schema.TimeSeriesSchema

class Analysis(uc : UserCube, sch : TimeSeriesSchema) {

    val list : List[String] = sch.columnList.filter(x => x._1 != "Time").map(x => x._1)

    def divideV2() : List[(Int, List[List[Possibility]])]  /*List[(Int, List[(String, String, String)])]*/= {
        def divideSubV2(i : Int, l : List[String], qV : List[(String, Int, List[String])]) : List[(Int, List[List[Possibility]])] /*List[(Int, List[(String, String, String)])]*/ = {
            if(sch.columns("Time").bits.toList.length == i){
                return Nil
            }
            val r : Array[Any] = uc.query(List(("Time", i, l)) ++ qV, Nil, resultForm = TUPLES_PREFIX) match {
                case x : Array[Any] => x
                case _ => Array[Any]()
            }
            val s : Array[(String, AnyVal)] = r.map(y => {
                y match {
                    case y : (String, AnyVal) => y
                    case _ => ("", 0L)
                }
            })
            //println("r =" + s.mkString(" "))
            /*val u : Array[String] = r.map(x => {
                x match {
                    case y : (String, Any) => y._1
                    case _ => ""
                }
            })
            val s = u.map(z => ArrayFunctions.findValueOfPrefix(z, "Time", true)).foldLeft(Array[String]()) {
                case (acc, item) if acc.contains(item) => acc
                case (acc, item) => acc ++ Array[String](item)
            }*/
            if(hasChange(s)){
                val z : (List[String], List[String]) = splitArrayV2(l)
                val prev : Int = z._1(z._1.length - 1).toInt
                val next : Int = z._2(0).toInt
                val x : List[(Int, List[(String, String, String)])] = detectedChange(uc.query(List(("Time", sch.columns("Time").bits.toList.length, List(prev.toString))) ++ qV, Nil, resultForm = TUPLES_PREFIX) match {
                case p : Array[Any] => p
                case _ => Array[Any]()
            }, uc.query(List(("Time", sch.columns("Time").bits.toList.length, List(next.toString))) ++ qV, Nil, resultForm = TUPLES_PREFIX) match {
                case p : Array[Any] => p
                case _ => Array[Any]()
            }, prev, next)
            x.foreach(w => {
            println("Line : " + w._1)
            w._2.foreach(p => {
                println("Change on column " + p._1 + " : " + p._2 + " ---> " + p._3)
            })
            })
                val y : List[(Int, List[List[Possibility]])] = if(x != Nil && !x.isEmpty) {
                    List(analyse(x(0)))
                }else Nil

                y.foreach(w => {
            println("Line : " + w._1)
            w._2.foreach(p => {
            println("Possibilities of " + w._1)
            p.foreach(q => {
                println(q + "   ")
            })
            })
            })
                return y ++ divideSubV2(i+1, z._1, qV) ++ divideSubV2(i+1, z._2, qV)
            }else return Nil
        }
        val max : Int = (Math.pow(2, sch.columns("Time").bits.toList.length).toInt - 1)
        val initList : List[String] = Range.inclusive(0, max).toList.map(_.toString)
        //A mettre pour tester à la fin mais affichage debug pour l'instant incompréhensible
        val qV : List[(String, Int, List[String])] = list.map(x => (x, 1, Nil))
        //val qV : List[(String, Int, List[String])] = List(("company", 1, Nil)) 
        return divideSubV2(0, initList, qV)
  }

  private def splitArrayV2(array : List[String]) : (List[String], List[String]) = {
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

  private def hasChange(array : Array[(String, AnyVal)]) : Boolean = {
      val inter : Array[String] = array.map(x => x._2.toString)
      val inter2 : Array[Double] = inter.map(x=> x.toDouble).filter(_!=0)
      return inter2.length > 1
  }

  private def detectedChange(prev : Array[Any], next : Array[Any], numberPrev : Int, numberNext : Int) : List[(Int, List[(String, String, String)])] = {
      val pr : Array[String] = prev.map(x => x match {
            case y : (String, AnyVal) => y
            case _ => ("", 0L)
      }).filter(x => x._2.toString.toDouble!=0).map(x=> x._1)
      val ne : Array[String] = next.map(x => x match {
            case y : (String, AnyVal) => y
            case _ => ("", 0L)
      }).filter(x => x._2.toString.toDouble!=0).map(x=> x._1)
      if(pr(0).split(";Time=" + numberPrev)(0) == ne(0).split(";Time=" + numberNext)(0)) {
          return Nil
      }
      var l : List[(String, String, String)] = Nil
      for(c <- list) {
          val valuePrev : String = ArrayFunctions.findValueOfPrefix(pr(0), c, false)
          val valueNext : String = ArrayFunctions.findValueOfPrefix(ne(0), c, false)
          if(valuePrev != valueNext) {
              l = l ++ List((c, valuePrev, valueNext))
          }
      }
      return List((numberNext, l))
  }

  def methodeN(tempList : List[List[Possibility]], l : List[Possibility], column : String, already : List[Possibility]) : List[List[Possibility]] = {
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
      return methodeN(list, l.tail, column, already ++ List(l.head))
  }

  def methodeNN(tempList : List[List[Possibility]], l : List[Possibility], column : String, already : List[Possibility]) : List[List[Possibility]] = {
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
      return methodeNN(list, l.tail, column, already ++ List(l.head))
  }

  def analyse(listChange : (Int, List[(String, String, String)])) : (Int, List[List[Possibility]]) = {
      var listAna : List[List[Possibility]] = Nil
      for(c <- listChange._2) {
          var tempList : List[List[Possibility]] = Nil
          if(listAna.isEmpty) {
              if(c._3 == "NULL") {
                  listAna = List(List(new Remove(c._1)))
              }else {
                    listAna = List(List(new Add(c._1)))
              }
          }else {
          for(l <- listAna){
              if(c._3 == "NULL") {
                  tempList = tempList ++ methodeN(Nil, l, c._1, Nil)
              }else {
                    tempList = tempList ++ methodeNN(Nil, l, c._1, Nil)
              }
          }
          listAna = tempList
          }
      }
      return (listChange._1, listAna)
  }
}

abstract class Possibility {
    override def toString() : String = {
        return "Possibilities"
    }
}
case class Add(x : String) extends Possibility {
    override def toString() : String = {
        return "Add the column " + x
    }
}
case class Rename(x : String, y : String) extends Possibility{
    override def toString() : String = {
        return "Rename the column " + x + " to the column " + y
    }
}
case class Remove(x : String) extends Possibility {
    override def toString() : String = {
        return "Remove the column " + x
    }
}

object Analysis {

}
