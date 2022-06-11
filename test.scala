import java.net.URL

import java.text.SimpleDateFormat
import java.text.ParseException
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.io._
import scala.util.control.Breaks._
import scala.io.StdIn.readLine
import frontend.JsonReader
import java.util.concurrent.TimeUnit
import frontend._
import backend._
import core._


object  test {

     def main(args: Array[String]): Unit = { 
    
       

       /* println("1234567890 : " + validateNumber("1234567890"));  
        println("1234 567 890 : " + validateNumber("1234 567 890"));   
        println("123 456 7890 : " + validateNumber("123 456 7890"));  
        println("123-567-8905 : " + validateNumber("123-567-8905"));  
        println("9866-767-545 : " + validateNumber("9866-767-545"));  
        println("9866.767.545 : " + validateNumber("9866.767.545"));  
        println("9866-767.545 : " + validateNumber("9866-767.545"));  
        println("123-456-7890 ext4444 : " + validateNumber("123-456-7890 ext4444"));  
        println("123-456-7890 x4444 : " + validateNumber("123-456-7890 x4444"));  
        println("9123124123  : " + validateNumber("9123124123"));  
        println("09123124123  : " + validateNumber("09123124123"));   
        println("+919123124123 : " + validateNumber("+919123124123"));  
        println("+91-9123124123 : " + validateNumber("+91-9123124123"));  
        println("+91 9222222222 : " + validateNumber("+91 9222222222"));  
        println("+7666 999 999 : " + validateNumber("+7666 999 999"));  
        println("(722) 037-8347 : " + validateNumber("(722) 037-8347"));  
       */

        //System.out.println(isValidURL("htps://www.inazumatv.fr/s2/100.html"))
        //println(genJsonFromStream());
       // println(readFromStream())
    /* var sc = ScalaBackend.initPartial()
     val sch = new schema.IsNullSchema
     var R = sch.read("investments.json")
      sc = ScalaBackend.mkPartial(sch.n_bits, R.toIterator,sc)
     R = sch.read("investments.json")
     sc = ScalaBackend.mkPartial(sch.n_bits, R.toIterator, sc)      
     val matscheme = RandomizedMaterializationScheme2(sch.n_bits, 8, 4, 4)
     val dc = new DataCube(matscheme)
     dc.build(sc)
     
    */
     /*val sch = new schema.IsNullSchema
     val dc = sch.readFromStream()
     println("test");
    print(sch.columnList.map(_._2.bits)); //(x,y) -> y.bits
    val qV = sch.columns("id").bits.toList //Company
    val qH = List()//even or odd years

    //FIXME: Replace query as Set[Int] instead of Seq[Int]. Until then, we assume query is sorted in increasing order of bits
    val q = (qV ++ qH).sorted

    // solves to df=2 using only 2-dim cuboids
    //val s = dc.solver[Rational](q, 2)
    //s.compute_bounds

    // runs up to the full cube
    val r = dc.naive_eval(q)
    println("r =" + r.mkString(" "))

    // this one need to run up to the full cube
   // val od = OnlineDisplay(sch, dc, PrettyPrinter.formatPivotTable(sch, qV, qH)) //FIXME: Fixed qV and qH. Make variable depending on query
    //od.l_run(q, 2)*/
    val sch = new schema.IsNullSchema
    val R = sch.read("investments.json")
    val basecuboid = CBackend.b.mk(sch.n_bits, R.toIterator)

    val matscheme = RandomizedMaterializationScheme2(sch.n_bits, 8, 4, 4)
    val dc = new DataCube(matscheme)
    dc.build(basecuboid)
    val cube = new UserCube(dc, sch)
    val analyse = new Analysis(cube, sch)
    //val r :  List[(Int, List[(String, String, String)])] = analyse.divideV2()
    /*r.foreach(x => {
        println("Line : " + x._1)
        x._2.foreach(y => {
            println("Change on column " + y._1 + " : " + y._2 + " ---> " + y._3)
        })
    })

    r.foreach(x => {
        println("Line : " + x._1)
        analyse.analyse(x)._2.foreach(y => {
            println("Possibilities of")
            y.foreach(z => {
                println(z + "   ")
            })
        })
    })*/
    val r :  List[(Int, List[List[Possibility]])] = analyse.divideV2()
    /*val r : List[Possibility] = List(Add("company"), Rename("date", "k_amount"), Remove("a"), Add("b"), Rename("c", "d"), Add("f"), Remove("g"))
    val resultNull : List[List[Possibility]] = analyse.methodeN(Nil, r, "personal", Nil)
    resultNull.foreach(w => {
        println("Possibilities of Null")
            w.foreach(q => {
                println(q + "   ")
            })
            })
        println()
        val resultNotNull : List[List[Possibility]] = analyse.methodeNN(Nil, r, "personal", Nil)
        resultNotNull.foreach(w => {
        println("Possibilities of Not Null")
            w.foreach(q => {
                println(q + "   ")
            })
            })*/
    /*val r = cube.query(List(("Time", sch.columns("Time").bits.toList.length, List("7"))) ++ List(("company", 1, Nil)), Nil, resultForm = TUPLES_PREFIX) match {
                case x : Array[Any] => x
                case _ => Array[Any]()
            }
    val s : Array[(String, AnyVal)] = r.map(y => {
                y match {
                    case y : (String, AnyVal) => y
                    case _ => ("", 0L)
                }
            })
            println("r =" + s.mkString(" "))*/
}


  def validateNumber(numberString : String): Boolean = {
        return "\\d{10}".r.unapplySeq(numberString).isDefined || //validates phone numbers having 10 digits
               "\\d{3}[-\\.\\s]\\d{3}[-\\.\\s]\\d{4}".r.unapplySeq(numberString).isDefined || //validates phone numbers having digits, -, . or spaces  
               "\\d{4}[-\\.\\s]\\d{3}[-\\.\\s]\\d{3}".r.unapplySeq(numberString).isDefined ||
               "\\d{3}-\\d{3}-\\d{4}\\s(x|(ext))\\d{3,5}".r.unapplySeq(numberString).isDefined || //validates phone numbers having digits and extension (length 3 to 5)  
               "\\(\\d{3}\\)-\\d{3}-\\d{4}".r.unapplySeq(numberString).isDefined || //validates phone numbers having digits and area code in braces  
               "\\(\\d{5}\\)-\\d{3}-\\d{3}".r.unapplySeq(numberString).isDefined ||
               "\\(\\d{4}\\)-\\d{3}-\\d{3}".r.unapplySeq(numberString).isDefined
    }


 def genJsonFromStream(url : String = "https://random-data-api.com/api/name/random_name"): String = {

    @volatile var qq = ""

    val thread  = new Thread {

         @volatile private var end = false

         def stopRunning(): Unit = end = true

         override def run {
            val fileWriter = new BufferedWriter(new FileWriter(new File("yo.json")));
            fileWriter.write("[")
            breakable {
                while(true) {
                    val co = new URL(url).openConnection;
                    val connection = co.asInstanceOf[HttpURLConnection]
                    val response = new StringBuilder();

                    try {
                        connection.setRequestMethod("GET");
                        val responseCode = connection.getResponseCode();
                        if (responseCode == HttpURLConnection.HTTP_OK) {
                            var line : String = ""
                            val bufferedReader = new BufferedReader(new InputStreamReader(connection.getInputStream())
                            );

                            line = bufferedReader.readLine()
                            while (line != null) {
                                
                                response.append(line);
                                fileWriter.write(line)
                                line = bufferedReader.readLine()
                            }
                            if(end) {
                                fileWriter.write("]")
                                connection.disconnect();
                                println("stream is closing...")
                            }
                            else {
                                fileWriter.write(",\n")
                            } 
                        }
                        else {
                            println("HTTP_NOT_OK")
                        }
                    } catch  {
                        case _: Throwable => println("io exception !")
                    } finally {
                        connection.disconnect();
                        if(end) {
                        println("done")
                        break
                        }
                    }
                }
            }
            qq = "aaa"
            fileWriter.close() 
         }
    }
       
    thread.start()
    println("Enter anything to stop !")
    val a = scala.io.StdIn.readLine
    thread.stopRunning()
    thread.join()
   return qq

    }

    def genJsonFromStream2(url : String = "https://random-data-api.com/api/name/random_name", chunckSize : Int = 5): Unit = {
        

    val thread  = new Thread {

         @volatile private var end = false

         def stopRunning(): Unit = end = true

         override def run {
           while(!end) {

                val fileWriter = new BufferedWriter(new FileWriter(new File("temp.json")));
                fileWriter.write("[")
        
                    for(i <- 1 to chunckSize) {
                        val co = new URL(url).openConnection;
                        val connection = co.asInstanceOf[HttpURLConnection]
                        val response = new StringBuilder();

                        try {
                            connection.setRequestMethod("GET");
                            val responseCode = connection.getResponseCode();
                            if (responseCode == HttpURLConnection.HTTP_OK) {
                                var line : String = ""
                                val bufferedReader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                            
                                line = bufferedReader.readLine()
                                while (line != null) {
                                    response.append(line);
                                    fileWriter.write(line)
                                    line = bufferedReader.readLine()
                                }
                                if(i == chunckSize) {
                                    fileWriter.write("]")
                                }
                                else {
                                    fileWriter.write(",\n")
                                } 
                            }
                            else {
                                println("wrong response code !")
                            }
                        } catch  {
                            case _: Throwable => println("io exception !")
                        } finally {
                            connection.disconnect();
                        }
                    }

                fileWriter.close()
                val items = JsonReader.read("temp.json")
                println(items.size)
                println("==================")

                }

                val fileTemp = new File("temp.json")
                if (fileTemp.exists) {
                fileTemp.delete()
                }
            }
    }
       
    thread.start()
    println("Enter anything to stop !")
    val a = scala.io.StdIn.readLine
    thread.stopRunning()
  

    }
   

     def readFromStream(measure_key: Option[String] = None, map_value : Object => Long = _.asInstanceOf[Long], url : String = "https://random-data-api.com/api/name/random_name", chunckSize : Int = 7): String = {
     
     @volatile var dc : String = ""
      val threadStream  = new Thread {

         @volatile private var end = false
  
         def stopRunning(): Unit = end = true

         override def run {

            val pathTemp1 : String = "temp1.json"
            val pathTemp2 : String  = "temp2.json"
            var pathWrite : String = pathTemp1
            var pathRead : String = pathTemp2

            def inversePaths(): Unit = {
              val pathTemp : String = pathWrite
              pathWrite = pathRead
              pathRead = pathTemp
              println("pathWrite = " + pathWrite + "  pathRead = " + pathRead)
            }
            def deleteFile(path : String): Unit = {
              var fileTemp = new File(path)
              if(fileTemp.exists) {
                fileTemp.delete()
              }
            }

            def getJsonArray(): Unit = {
              val fileWriter = new BufferedWriter(new FileWriter(new File(pathWrite)));
              fileWriter.write("[")
              for(i <- 1 to chunckSize) {
                val co = new URL(url).openConnection;
                val connection = co.asInstanceOf[HttpURLConnection]
                val response = new StringBuilder();
                try {
                    connection.setRequestMethod("GET");
                    val responseCode = connection.getResponseCode();
                    if (responseCode == HttpURLConnection.HTTP_OK) {
                        var line : String = ""
                        val bufferedReader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                                
                        line = bufferedReader.readLine()
                        while(line != null) {
                            for(str <- line.toString().split(',')) {
                                    if(str.startsWith("\"first_name\":")) {
                                        val name = str.replace("\"first_name\":","").replace("\"","")
                                        println(name)
                                    }
                            }
                          response.append(line);
                          fileWriter.write(line)
                          line = bufferedReader.readLine()
                        }
                        if(i == chunckSize) {
                          fileWriter.write("]")
                        }
                        else {
                          fileWriter.write(",\n")
                        } 
                    }
                    else {
                      println("wrong HTTP response code !")
                    }
                  } catch {
                      case _: Throwable => println("io exception !")
                  } finally {
                      connection.disconnect();
                  }
              }
              fileWriter.close()
            }

            def getThreadCube(): Thread = {
                new Thread {
                    override def run {
                        val items = JsonReader.read(pathRead)
                        dc = "a"
                        for (a <- items){
                            println(a.get("first_name"))
                        }
                    }
                }
            }

            var threadCube = getThreadCube()
            //==================================
            getJsonArray()

            inversePaths()

            while(!end) {
              threadCube.start()
              getJsonArray()
              threadCube.join()
              inversePaths()
              threadCube = getThreadCube()
            }
            threadCube.start()
            deleteFile(pathWrite)
            threadCube.join()
            deleteFile(pathRead)
          }
     }
       
    threadStream.start()
    println("Enter anything to stop !")
    scala.io.StdIn.readLine()
    threadStream.stopRunning()
    threadStream.join()
    dc
 }
    
}
