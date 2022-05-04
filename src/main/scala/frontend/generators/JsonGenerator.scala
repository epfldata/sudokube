package frontend.generators

import scala.util.Random
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import java.io._
import org.apache.commons.lang3.RandomStringUtils;
import java.time.Year

object JsonGenerator {
    def main(args: Array[String]): Unit = {
        JsonWriter.gen("random.json", 100, List(SimpleField("date", DateGenerator()),SimpleField("email", EmailGenerator()), SimpleFieldInt("id", IntGenerator()), NestedJson("nested", List(SimpleField("name", NameGenerator())))))
  }
}

object JsonWriter {
    
    def gen(filename : String, length : Int, schema : List[Field]) : Unit = {

        val mapper = new ObjectMapper() with ScalaObjectMapper
        mapper.registerModule(DefaultScalaModule)
        val file = new File(filename)
        val fileWriter = new FileWriter(file, true)
        val sequenceWriter = mapper.writerWithDefaultPrettyPrinter().writeValuesAsArray(fileWriter)

        for(i <- 0 to length - 1) {
             val map = scala.collection.mutable.Map[String,Any]()
            for (f <- schema) {
                f match {
                    case SimpleField(key,value) => map.put(key, value.generate())
                    case SimpleFieldInt(key, value) => map.put(key, value.generate())
                    case SimpleFieldDouble(key, value) => map.put(key, value.generate())
                    case SimpleFieldFloat(key, value) => map.put(key, value.generate())
                    case NestedJson(key, value) => map.put(key, mapNesterJson(value))
                }
            }
             sequenceWriter.write(map)
        }
        sequenceWriter.close()
    }

    private def mapNesterJson(schema : List[Field]) : scala.collection.mutable.Map[String,Any] = {

        val map = scala.collection.mutable.Map[String,Any]()
        for(f <- schema) {
            f match {
                case SimpleField(key,value) => map.put(key, value.generate())
                case SimpleFieldInt(key, value) => map.put(key, value.generate())
                case SimpleFieldDouble(key, value) => map.put(key, value.generate())
                case SimpleFieldFloat(key, value) => map.put(key, value.generate())
                case NestedJson(key, value) => map.put(key, mapNesterJson(value))
            }
                
        }
        map
    }
      
}


abstract class Field

case class SimpleField(key : String, value : MyGenerator[String]) extends Field
case class SimpleFieldInt(key : String, value : MyGenerator[Int]) extends Field
case class SimpleFieldDouble(key : String, value : MyGenerator[Double]) extends Field
case class SimpleFieldFloat(key : String, value : MyGenerator[Float]) extends Field
case class NestedJson(key : String, value : List[Field]) extends Field


object Order {
    val YEAR_FIRST = 0
    val DAY_FIRST = 1
    val RANDOM = 2
}

abstract class MyGenerator[T]() {
    def generate() : T
}

case class EmailGenerator(hostName : String = "gmail.com", localEmailLength: Int = 6) extends MyGenerator[String] {
    private val ALLOWED_CHARS : String = "abcdefghijklmnopqrstuvwxyz" + "1234567890" + "_-."

    def generate() : String = {
        return RandomStringUtils.random(localEmailLength, ALLOWED_CHARS) + "@" + hostName;
    }
}

case class NameGenerator(length : Int = 4) extends MyGenerator[String] {
     private val ALLOWED_CHARS : String = "abcdefghijklmnopqrstuvwxyz"

     def generate() : String = {
       return RandomStringUtils.random(length, ALLOWED_CHARS)
    }
}

case class DateGenerator(separetor : Char = '/', yearBegin : Int = 1903, order : Int = Order.RANDOM) extends MyGenerator[String] {
    private val months = Map((1,31), (2,29), (3,31), (4, 30),(5,31), (6,30), (7,31), (8,31), (9,30), (10,31), (11, 31), (12,31))

    def generate() : String = {
        val year = IntGenerator(1900,Year.now.getValue).generate()
        var month = IntGenerator(1,12).generate()
        var day = 0
        if(month == 2 && year % 4 == 0) {
            day = IntGenerator(1,28).generate()
        }
        else {
             day = IntGenerator(1,months.getOrElse(month,28)).generate()
        }

        var dayStr = ""
        if(day < 10) {
            dayStr = "0" + day.toString()
        }
        else {
            dayStr = day.toString()
        }

        var monthStr = ""
        if(month < 10) {
            monthStr = "0" + month.toString()
        }

        else {
            monthStr = month.toString()
        }

        var yearStr = ""
        if(year < 10) {
            yearStr = "0" + year.toString()
        }
        else {
            yearStr = year.toString()
        }
        
        if(order == Order.YEAR_FIRST) {
              return yearStr + separetor + monthStr + separetor + dayStr
        }
        else if(order == Order.DAY_FIRST) {
            return dayStr + separetor + monthStr + separetor + yearStr
        }
        else {
            val rdm = Random.nextInt(2)

            if(rdm == 0) {
                return yearStr + separetor + monthStr + separetor + dayStr
            }
            else {
                return dayStr + separetor + monthStr + separetor + yearStr
            }
        }
    }

}

case class IntGenerator(start : Int = 1, end : Int = 10) extends MyGenerator[Int] {
     def generate() : Int = {
        return start + Random.nextInt( (end - start) + 1 ) 
    }
}






