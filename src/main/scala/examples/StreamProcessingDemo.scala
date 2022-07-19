package examples

import backend.CBackend
import core.DataCube
import core.materialization.RandomizedMaterializationScheme
import frontend._

import java.io._
import java.net.{HttpURLConnection, URL}
import scala.util.control.Breaks.{break, breakable}

object StreamProcessingDemo {

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


    // this one need to run up to the full cube
    // val od = OnlineDisplay(sch, dc, PrettyPrinter.formatPivotTable(sch, qV, qH)) //FIXME: Fixed qV and qH. Make variable depending on query
    //od.l_run(q, 2)*/
    val sch = new schema.IsNullSchema
    val R = sch.read("random.json")
    val basecuboid = CBackend.b.mk(sch.n_bits, R.toIterator)

    val matscheme = new RandomizedMaterializationScheme(sch.n_bits, 8, 4)
    val dc = new DataCube()
    dc.build(basecuboid, matscheme)
    val cube = new UserCube("StreamDemo", dc, sch)
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

    val r: List[(Int, List[List[Possibility]])] = analyse.analyse()

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


  def validateNumber(numberString: String): Boolean = {
    return "\\d{10}".r.unapplySeq(numberString).isDefined || //validates phone numbers having 10 digits
      "\\d{3}[-\\.\\s]\\d{3}[-\\.\\s]\\d{4}".r.unapplySeq(numberString).isDefined || //validates phone numbers having digits, -, . or spaces
      "\\d{4}[-\\.\\s]\\d{3}[-\\.\\s]\\d{3}".r.unapplySeq(numberString).isDefined ||
      "\\d{3}-\\d{3}-\\d{4}\\s(x|(ext))\\d{3,5}".r.unapplySeq(numberString).isDefined || //validates phone numbers having digits and extension (length 3 to 5)
      "\\(\\d{3}\\)-\\d{3}-\\d{4}".r.unapplySeq(numberString).isDefined || //validates phone numbers having digits and area code in braces
      "\\(\\d{5}\\)-\\d{3}-\\d{3}".r.unapplySeq(numberString).isDefined ||
      "\\(\\d{4}\\)-\\d{3}-\\d{3}".r.unapplySeq(numberString).isDefined
  }


  def genJsonFromStream(url: String = "https://random-data-api.com/api/name/random_name"): String = {

    @volatile var qq = ""

    val thread = new Thread {

      @volatile private var end = false

      def stopRunning(): Unit = end = true

      override def run {
        val fileWriter = new BufferedWriter(new FileWriter(new File("yo.json")));
        fileWriter.write("[")
        breakable {
          while (true) {
            val co = new URL(url).openConnection;
            val connection = co.asInstanceOf[HttpURLConnection]
            val response = new StringBuilder();

            try {
              connection.setRequestMethod("GET");
              val responseCode = connection.getResponseCode();
              if (responseCode == HttpURLConnection.HTTP_OK) {
                var line: String = ""
                val bufferedReader = new BufferedReader(new InputStreamReader(connection.getInputStream())
                );

                line = bufferedReader.readLine()
                while (line != null) {

                  response.append(line);
                  fileWriter.write(line)
                  line = bufferedReader.readLine()
                }
                if (end) {
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
            } catch {
              case _: Throwable => println("io exception !")
            } finally {
              connection.disconnect();
              if (end) {
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

  def genJsonFromStream2(url: String = "https://random-data-api.com/api/name/random_name", chunckSize: Int = 5): Unit = {


    val thread = new Thread {

      @volatile private var end = false

      def stopRunning(): Unit = end = true

      override def run {
        while (!end) {

          val fileWriter = new BufferedWriter(new FileWriter(new File("temp.json")));
          fileWriter.write("[")

          for (i <- 1 to chunckSize) {
            val co = new URL(url).openConnection;
            val connection = co.asInstanceOf[HttpURLConnection]
            val response = new StringBuilder();

            try {
              connection.setRequestMethod("GET");
              val responseCode = connection.getResponseCode();
              if (responseCode == HttpURLConnection.HTTP_OK) {
                var line: String = ""
                val bufferedReader = new BufferedReader(new InputStreamReader(connection.getInputStream()));

                line = bufferedReader.readLine()
                while (line != null) {
                  response.append(line);
                  fileWriter.write(line)
                  line = bufferedReader.readLine()
                }
                if (i == chunckSize) {
                  fileWriter.write("]")
                }
                else {
                  fileWriter.write(",\n")
                }
              }
              else {
                println("wrong response code !")
              }
            } catch {
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


  def readFromStream(measure_key: Option[String] = None, map_value: Object => Long = _.asInstanceOf[Long], url: String = "https://random-data-api.com/api/name/random_name", chunckSize: Int = 7): String = {

    @volatile var dc: String = ""
    val threadStream = new Thread {

      @volatile private var end = false

      def stopRunning(): Unit = end = true

      override def run {

        val pathTemp1: String = "temp1.json"
        val pathTemp2: String = "temp2.json"
        var pathWrite: String = pathTemp1
        var pathRead: String = pathTemp2

        def inversePaths(): Unit = {
          val pathTemp: String = pathWrite
          pathWrite = pathRead
          pathRead = pathTemp
          println("pathWrite = " + pathWrite + "  pathRead = " + pathRead)
        }

        def deleteFile(path: String): Unit = {
          var fileTemp = new File(path)
          if (fileTemp.exists) {
            fileTemp.delete()
          }
        }

        def getJsonArray(): Unit = {
          val fileWriter = new BufferedWriter(new FileWriter(new File(pathWrite)));
          fileWriter.write("[")
          for (i <- 1 to chunckSize) {
            val co = new URL(url).openConnection;
            val connection = co.asInstanceOf[HttpURLConnection]
            val response = new StringBuilder();
            try {
              connection.setRequestMethod("GET");
              val responseCode = connection.getResponseCode();
              if (responseCode == HttpURLConnection.HTTP_OK) {
                var line: String = ""
                val bufferedReader = new BufferedReader(new InputStreamReader(connection.getInputStream()));

                line = bufferedReader.readLine()
                while (line != null) {
                  for (str <- line.toString().split(',')) {
                    if (str.startsWith("\"first_name\":")) {
                      val name = str.replace("\"first_name\":", "").replace("\"", "")
                      println(name)
                    }
                  }
                  response.append(line);
                  fileWriter.write(line)
                  line = bufferedReader.readLine()
                }
                if (i == chunckSize) {
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
              for (a <- items) {
                println(a.get("first_name"))
              }
            }
          }
        }

        var threadCube = getThreadCube()
        //==================================
        getJsonArray()

        inversePaths()

        while (!end) {
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
