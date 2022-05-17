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
        //JsonWriter.gen("random.json", 100, List(SimpleField("date", DateGenerator()),SimpleField("email", EmailGenerator()), SimpleFieldInt("id", IntGenerator()), NestedJson("nested", List(SimpleField("name", NameGenerator())))))
        
        // The 10 most used email host names
        val emailHostnames : Seq[String] = Seq(
            "gmail.com",
            "yahoo.com", 
            "hotmail.com", 
            "aol.com", 
            "hotmail.co.uk", 
            "hotmail.fr", 
            "msn.com", 
            "yahoo.fr", 
            "wanadoo.fr", 
            "orange.fr"
        );

        val jsonWriter = new JsonWriter(List(SimpleField("date", DateGenerator(format = Format.YEAR))), 6)
        jsonWriter.modifySchema(List(SimpleField("dateY", DateGenerator(format = Format.YEAR_MONTH))), 1)
        jsonWriter.modifySchema(List(SimpleField("dateY", DateGenerator(format = Format.DATE))), 2)
        jsonWriter.modifySchema(List(SimpleField("dateY", DateGenerator(format = Format.HOUR))), 3)
        jsonWriter.modifySchema(List(SimpleField("dateY", DateGenerator(format = Format.MINUTE))), 4)
        jsonWriter.modifySchema(List(SimpleField("dateY", DateGenerator(format = Format.SECONDS))), 5)
        jsonWriter.gen("random.json")
  }
}

class JsonWriter(var currentSchema : List[Field], val numberOfLign : Int) {

    var mapSchema = scala.collection.mutable.Map[Int,List[Field]]()
    mapSchema.put(0, currentSchema)
    
    def gen(filename : String) : Unit = {

        val mapper = new ObjectMapper() with ScalaObjectMapper
        mapper.registerModule(DefaultScalaModule)
        val file = new File(filename)
        val fileWriter = new FileWriter(file, true)
        val sequenceWriter = mapper.writerWithDefaultPrettyPrinter().writeValuesAsArray(fileWriter)

        for(i <- 0 to numberOfLign - 1) {
            if(mapSchema.keySet.contains(i)){
                currentSchema = mapSchema.getOrElse(i, currentSchema);
            }
            val map = convert()
            sequenceWriter.write(map)
        }
        sequenceWriter.close()
    }

    def modifySchema(newSchema : List[Field], lignOfModification : Int) : Unit = {
        if(lignOfModification >= 0 && lignOfModification < numberOfLign){
            mapSchema.put(lignOfModification, newSchema)
        }
    }

    private def convert() = {
        val map = scala.collection.mutable.Map[String,Any]()
            for (f <- currentSchema) {
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


abstract class MyGenerator[T]() {
    def generate() : T
}

case class EmailGenerator(hostNames : Seq[String] = Seq("gmail.com", "yahoo.com", "hotmail.com"), localEmailLength: Int = 6) extends MyGenerator[String] {
    private val ALLOWED_CHARS : String = "abcdefghijklmnopqrstuvwxyz" + "1234567890" + "_-."

    def generate() : String = {
        return RandomStringUtils.random(localEmailLength, ALLOWED_CHARS) + "@" + hostNames(Random.nextInt(hostNames.length))
    }
}

case class NameGenerator(length : Int = 4, private var ALLOWED_CHARS : String = "abcdefghijklmnopqrstuvwxyz") extends MyGenerator[String] {

     def generate() : String = {
       return RandomStringUtils.random(length, ALLOWED_CHARS)
    }
}

object Format {
        val DATE = 0
        val HOUR = 1
        val MINUTE = 4
        val SECONDS = 5
        val YEAR = 2
        val YEAR_MONTH = 3
    }

    object Order {
        val YEAR_FIRST = 0
        val DAY_FIRST = 1
        val RANDOM = 2
    }

case class DateGenerator(separetor : Char = '/', yearBegin : Int = 1903, order : Int =  Order.RANDOM, format : Int = Format.DATE) extends MyGenerator[String] {
    private val months = Map((1,31), (2,28), (3,31), (4, 30),(5,31), (6,30), (7,31), (8,31), (9,30), (10,31), (11, 31), (12,31))

    def generateYear() : String = {
        IntGenerator(1900,Year.now.getValue).generate().toString()
    }

    def generateYM() : String = {
        val year = generateYear()
        val separator = NameGenerator(1, "/-").generate()
        val month = formatValue(IntGenerator(1, 12).generate())
        formatDate(year, separator, month)
    }

    private def formatValue(value : Int) : String = {
        if(value < 10) {
            "0" + value.toString()
        }else 
            value.toString()
    }

    private def formatDate(year : String, separator : String, month : String, day : String = "") : String = {
        val randomBoolean = IntGenerator(0,1).generate()
        if(order == Order.YEAR_FIRST) {
            val end = if(day.isEmpty()) "" else separator + day
            year + separator + month + end
        }
        else if(order == Order.DAY_FIRST) {
            val end = if(day.isEmpty()) "" else day + separator
            end + month + separator + year
        }
        else { 
            if(randomBoolean == 0){
                val end = if(day.isEmpty()) "" else separator + day
                year + separator + month + end
            }else{
                val end = if(day.isEmpty()) "" else day + separator
                end + month + separator + year
            }
        }
    }

    def generate() : String = {
        format match {
            case Format.DATE => generateDate()
            case Format.HOUR => generateDateWithHour()
            case Format.MINUTE => generateMin()
            case Format.SECONDS => generateSec()
            case Format.YEAR => generateYear()
            case Format.YEAR_MONTH => generateYM()
        }
    }

    def generateDateWithHour() : String = {
        val date = generateDate()
        val hour = formatValue(IntGenerator(0,23).generate())
        date + "T" + hour + ":00:00.000Z"
    }

    def generateSec() : String = {
        val date = generateDate()
        val hour = formatValue(IntGenerator(0,23).generate())
        val separator = ":"
        val gen = IntGenerator(0, 59)
        val min = formatValue(gen.generate())
        var sec = formatValue(gen.generate())
        val miniSec = IntGenerator(0, 999).generate()
        var miniSecStr : String = formatValue(miniSec)
        if(miniSec < 100){
            miniSecStr = "0" + miniSecStr
        }
        sec = sec + "." + miniSecStr
        date + "T" + hour + separator + min + separator + sec + "Z"
    }

    def generateMin() : String = {
        val date = generateDate()
        val hour = formatValue(IntGenerator(0,23).generate())
        val separator = ":"
        val gen = IntGenerator(0, 59)
        val min = formatValue(gen.generate())
        date + "T" + hour + separator + min + separator + "00.000Z"
    }

    def generateDate() : String = {
        val year = IntGenerator(1900,Year.now.getValue).generate()
        val month = IntGenerator(1,12).generate()
        var day = 0
        if(month == 2 && year % 4 == 0 && year % 100 != 0) {
            day = IntGenerator(1,29).generate()
        }
        else {
             day = IntGenerator(1,months.getOrElse(month,28)).generate()
        }
        val dayStr = formatValue(day)
        val monthStr = formatValue(month)
        val yearStr = year.toString()
        val separator = NameGenerator(1, "/-").generate()
        formatDate(yearStr, separator, monthStr, dayStr)
    }

}

case class IntGenerator(start : Int = 1, end : Int = 10) extends MyGenerator[Int] {
     def generate() : Int = {
        return start + Random.nextInt( (end - start) + 1 ) 
    }
}

case class phoneNumberGenerator() {
      def generate() : String = {
        return RandomStringUtils.random(10, "1234567890")
    }
    
}






