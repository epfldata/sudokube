package frontend.generators

import scala.util.Random
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import java.io._
import org.apache.commons.lang3.RandomStringUtils;
import java.time.Year
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

object JsonGenerator {
    def main(args: Array[String]): Unit = {
        //JsonWriter.gen("random.json", 100, List(SimpleField("date", DateGenerator()),SimpleField("email", EmailGenerator()), SimpleFieldInt("id", IntGenerator()), NestedJson("nested", List(SimpleField("name", NameGenerator())))))
        
        // The 10 most used email host names
        /*val emailHostnames : Seq[String] = Seq(
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
        new JsonWriter().gen(10, "random.json", 5)*/
        println("Enter the number of line : ")
        val nbLine : Int = scala.io.StdIn.readLine().toInt
        println("Enter the filename : ")
        val filename : String = scala.io.StdIn.readLine()
        println("Enter the number of column : ")
        val nbCol : Int = scala.io.StdIn.readLine().toInt
        new JsonWriter().gen(nbLine, filename, nbCol)
  }
}

class JsonWriter() {
    
    def gen(numberOfLign : Int, filename : String, initSizeOfSchema : Int) : Unit = {

        val mapper = new ObjectMapper() with ScalaObjectMapper
        mapper.registerModule(DefaultScalaModule)
        val file = new File(filename)
        val fileWriter = new FileWriter(filename)
        val sequenceWriter = mapper.writerWithDefaultPrettyPrinter().writeValuesAsArray(fileWriter)
        var currentSchema : List[Field] = initSchema(initSizeOfSchema)
        sequenceWriter.write(convert(currentSchema))

        for(i <- 1 to numberOfLign - 1) {
            val schema : List[Field] = modifySchema(currentSchema)
            val map = convert(schema)
            currentSchema = schema
            sequenceWriter.write(map)
        }
        sequenceWriter.close()
    }

    def initSchema(initSizeOfSchema : Int) : List[Field] = {
        var list : List[Field] = List()
        var i = 1
        while(i <= initSizeOfSchema){
            val random : Int = new IntGenerator(0, 10).generate()
            val key : String = generateKey(list)
            if(random <= 1 && initSizeOfSchema-i > 1) {
                val j : Int = new IntGenerator(1, initSizeOfSchema-i).generate()
                list = list ++ List(new NestedJson(key, initSchema(j)))
                i = i + j
            }else {
                val field : Field = initField(key)
                list = list ++ List(field)
                i = i + 1
            }
        }
        list
    }

    def initField(key : String) : Field = {
        val randomType = new IntGenerator(0,1).generate()
                
        if(randomType == 0){
            new SimpleFieldInt(key, new IntGenerator(0, 2022))
        }else {
            val randomString = new IntGenerator(0, 6).generate()
            randomString match {
                case 0 => new SimpleField(key, new EmailGenerator())
                case 1 => new SimpleField(key, new NameGenerator())
                case 2 => new SimpleField(key, new DateGenerator())
                case 3 => new SimpleField(key, new phoneNumberGenerator())
                case 4 => new SimpleField(key, new filePathGenerator(lineNumber = new IntGenerator(0, 0)))
                case 5 => new SimpleField(key, new filePathGenerator(lineNumber = new IntGenerator(1, 500)))
                case _ => new SimpleField(key, new StringGenerator())
            }
        }
                
    }

    def containsKey(l : List[Field], k : String) : Boolean = {
        if(l.isEmpty) {
            false
        } else if(l.head.getKey().equals(k)) {
            true
        } else {
            containsKey(l.tail, k)
        }
    }

    def modifySchema(currentSchema : List[Field]) : List[Field] = {
        val randomChanging = new IntGenerator(0, 4).generate()
        if(randomChanging <= 1){
            val random = new IntGenerator(0, 2).generate()
            if(currentSchema.isEmpty || currentSchema.length == 1){
                addField(currentSchema)
            }else {
                random match {
                    case 0 => addField(currentSchema)
                    case 1 => removeField(currentSchema)
                    case 2 => renameField(currentSchema)
                }
            }
        }else {
            currentSchema
        }
    }

    def renameField(currentSchema : List[Field]) : List[Field] = {
        val random = new IntGenerator(0, currentSchema.length-1).generate()
        var list : (List[Field], List[Field]) = currentSchema.splitAt(random)
        list._2.head match {
            case NestedJson(key, value) => {
                val newField = new NestedJson(key, renameField(value))
                list._1 ++ List(newField) ++ list._2.tail
            }
            case SimpleField(key, value) => {
                val k : String = generateKey(currentSchema)
                list._1 ++ List(new SimpleField(k, value)) ++ list._2.tail
            }
            case SimpleFieldDouble(key, value) => {
                val k : String = generateKey(currentSchema)
                list._1 ++ List(new SimpleFieldDouble(k, value)) ++ list._2.tail
            }
            case SimpleFieldFloat(key, value) => {
                val k : String = generateKey(currentSchema)
                list._1 ++ List(new SimpleFieldFloat(k, value)) ++ list._2.tail
            }
            case SimpleFieldInt(key, value) => {
                val k : String = generateKey(currentSchema)
                list._1 ++ List(new SimpleFieldInt(k, value)) ++ list._2.tail
            }
        }
    }

    def generateKey(currentSchema : List[Field]) : String = {
        val gkey : NameGenerator = new NameGenerator()
        var k : String = ""
        do {
            k = gkey.generate()
        }while(containsKey(currentSchema, k))
        k
    }

    def addField(currentSchema : List[Field]) : List[Field] = {
        val key : String = generateKey(currentSchema)
        currentSchema ++ List(initField(key))
    }

    def removeField(currentSchema : List[Field]) : List[Field] = {
        val random = new IntGenerator(0, currentSchema.length-1).generate()
        var list : (List[Field], List[Field]) = currentSchema.splitAt(random)
        list._2.head match {
            case NestedJson(key, value) => {
                val newList = removeField(value)
                if(newList.isEmpty){
                    list._1 ++ list._2.tail
                }else{
                    val newField = new NestedJson(key, newList)
                    list._1 ++ List(newField) ++ list._2.tail
                }
            }
            case _ => list._1 ++ list._2.tail
        }
    }

    private def convert(currentSchema : List[Field]) = {
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


abstract class Field(key : String) {
    def getKey() : String = key
}

case class SimpleField(key : String, value : MyGenerator[String]) extends Field(key : String)
case class SimpleFieldInt(key : String, value : MyGenerator[Int]) extends Field(key : String)
case class SimpleFieldDouble(key : String, value : MyGenerator[Double]) extends Field(key : String)
case class SimpleFieldFloat(key : String, value : MyGenerator[Float]) extends Field(key : String)
case class NestedJson(key : String, value : List[Field]) extends Field(key : String)


abstract class MyGenerator[T]() {
    def generate() : T

}

case class EmailGenerator(hostNames : Seq[String] = Seq("gmail.com", "yahoo.com", "hotmail.com"), localEmailLength: Int = 6) extends MyGenerator[String] {
    private val ALLOWED_CHARS : String = "abcdefghijklmnopqrstuvwxyz" + "1234567890" + "_-."

    def generate() : String = {
        val co = new URL("https://random-data-api.com/api/users/random_user").openConnection
        val connection = co.asInstanceOf[HttpURLConnection]
        val response = new StringBuilder()
        var res = "a.b@gmail.com"
        try {

            connection.setRequestMethod("GET");

            val responseCode = connection.getResponseCode()
            if (responseCode == HttpURLConnection.HTTP_OK) {
                var line : String = ""
                val bufferedReader = new BufferedReader(
                        new InputStreamReader(connection.getInputStream())
                );

                line = bufferedReader.readLine()
                while (line != null) {
                    response.append(line);
                    line = bufferedReader.readLine()
                }
                for(str <- response.toString().split(',')) {
                    if(str.startsWith("\"email\":")) {
                       val name = str.replace("\"email\":","").replace("\"","").replace("@email.com","")
                        res = name + "@" + hostNames(Random.nextInt(hostNames.length))

                    }
                }
                bufferedReader.close()
                connection.disconnect();
                return res

            }
            else {
                connection.disconnect()
                return res 
            }
           
        } catch  {
            case _: Throwable => { connection.disconnect()
                                   return res }
        }

    }
}

case class NameGenerator() extends MyGenerator[String] {

     def generate() : String = {
       val co = new URL("https://random-data-api.com/api/name/random_name").openConnection
        val connection = co.asInstanceOf[HttpURLConnection]
        val response = new StringBuilder()
        var res = "Hugo"
        try {

            connection.setRequestMethod("GET");

            val responseCode = connection.getResponseCode()
            if (responseCode == HttpURLConnection.HTTP_OK) {
                var line : String = ""
                val bufferedReader = new BufferedReader(
                        new InputStreamReader(connection.getInputStream())
                );

                line = bufferedReader.readLine()
                while (line != null) {
                    response.append(line);
                    line = bufferedReader.readLine()
                }
                for(str <- response.toString().split(',')) {
                    if(str.startsWith("\"first_name\":")) {
                       val name = str.replace("\"first_name\":","").replace("\"","")
                        res = name

                    }
                }
                bufferedReader.close()
                connection.disconnect();
                return res

            }
            else {
                connection.disconnect()
                return res 
            }
           
        } catch  {
            case _: Throwable => { connection.disconnect()
                                   return res }
        }

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

case class StringGenerator(length : Int = 4, private var ALLOWED_CHARS : String = "abcdefghijklmnopqrstuvwxyz") extends MyGenerator[String] {

     def generate() : String = {
       return RandomStringUtils.random(length, ALLOWED_CHARS)
    }
}

case class DateGenerator(separetor : Char = '/', yearBegin : Int = 1903, order : Int =  Order.RANDOM, format : Int = Format.DATE) extends MyGenerator[String] {
    private val months = Map((1,31), (2,28), (3,31), (4, 30),(5,31), (6,30), (7,31), (8,31), (9,30), (10,31), (11, 31), (12,31))

    def generateYear() : String = {
        IntGenerator(1900,Year.now.getValue).generate().toString()
    }

    def generateYM() : String = {
        val year = generateYear()
        val separator = StringGenerator(1, "/-").generate()
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
        val separator = StringGenerator(1, "/-").generate()
        formatDate(yearStr, separator, monthStr, dayStr)
    }

}

case class IntGenerator(start : Int = 1, end : Int = 10) extends MyGenerator[Int] {
     def generate() : Int = {
        return start + Random.nextInt( (end - start) + 1 ) 
    }
}

case class phoneNumberGenerator() extends MyGenerator[String]{
      def generate() : String = {
        return RandomStringUtils.random(10, "1234567890")
    }
    
}
case class filePathGenerator(numberParent : Int = 2, extensionName : Seq[String] = Seq(".json", ".scala", ".txt", ".java"), lineNumber : IntGenerator)  extends MyGenerator[String]{
    def generate() : String = {
        var filePath : String = "/"
        for(i <- 1 until numberParent){
            val generateName = NameGenerator()
            filePath += generateName.generate() + "/"
        }
        filePath += NameGenerator().generate() + extensionName(Random.nextInt(extensionName.length))
        val number : Int = lineNumber.generate()
        if(number > 0){
            filePath += ":" + number.toString()
        }
        filePath
    }
}