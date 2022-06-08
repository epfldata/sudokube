//package ch.epfl.data.sudokube
package frontend
package schema

import breeze.io.CSVReader
import util._
import util.BigBinaryTools._
import combinatorics.Big
import frontend.schema.encoders.ColEncoder
import java.net.HttpURLConnection;
import java.net.URL;
import core._
import backend._


import java.io._

@SerialVersionUID(6L)
trait Schema extends Serializable {
  // abstract members
  def n_bits: Int

  def columnList: List[(String, ColEncoder[_])]
  protected def encode_column(key: String, v: Any) : BigBinary

  def encode_tuple(t: Seq[(String, Any)]): BigBinary = {
    val cols = Profiler("EncodeColumn"){(t.map { case (key, v) => encode_column(key, v) })}
      Profiler("ColumnSum"){cols.sum}
  }

  def decode_tuple(i: BigBinary): Seq[(String, Any)] =
    columnList.map { case (key, c) => (key, c.decode(i)) }

  def read(filename: String, measure_key: Option[String] = None, map_value : Object => Long = _.asInstanceOf[Long]
          ): Seq[(BigBinary, Long)] = {

    val items = {
      if(filename.endsWith("json"))
        JsonReader.read(filename)
      else if(filename.endsWith("csv")){

        val csv = Profiler("CSVRead"){CSVReader.read(new FileReader(filename))}
        val header = csv.head
        val rows = Profiler("AddingColNames"){csv.tail.map(vs => header.zip(vs).toMap)}
        rows
      } else
        throw new UnsupportedOperationException("Only CSV or JSON supported")
    }

    if (measure_key == None) {
      items.map(l => (encode_tuple(l.toList), 1L))
    }
    else {
      items.map(l => {
        val x = l.toMap
        val measure = x.get(measure_key.get).map(map_value).getOrElse(0L)
        (encode_tuple((x - measure_key.get).toList), measure)
      })
    }
  }

  /** saves the schema as a file. Note: Schema.load is used to load, not
      read()!
   */
  def save(filename: String) {
    val file = new File("cubedata/"+filename+"/"+filename+".sch")
    if(!file.exists())
      file.getParentFile.mkdirs()
    val oos = new ObjectOutputStream(new FileOutputStream(file))
    oos.writeObject(this)
    oos.close
  }

  def decode_dim(q_bits: List[Int]): Seq[Seq[String]] = {
    val relevant_cols = columnList.filter(!_._2.bits.intersect(q_bits).isEmpty)
    val universe = relevant_cols.map {
      case (_, c) => c.bits
    }.flatten.sorted

    Bits.group_values(q_bits, universe).map(
      x => relevant_cols.map {
        case (key, c) => {
          val l = x.map(y =>
            try {
              val w = c.decode(y) match {
                case Some(u) => u.toString
                case None => "NULL"
                case u => u.toString
              }
              Some(w)
            }
            catch {
              case e: Exception => None
            }
          ).flatten.toSet.toList.sorted // duplicate elimination

          if (l.length == 1) key + "=" + l(0)
          else key + " in " + l
        }
      })
  }

  def readFromStream(measure_key: Option[String] = None, map_value : Object => Long = _.asInstanceOf[Long], url : String = "https://random-data-api.com/api/name/random_name", chunckSize : Int = 7): DataCube = {
     
     @volatile var sc = ScalaBackend.initPartial()
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
                        var r = read(pathRead)
                        sc = ScalaBackend.mkPartial(n_bits, r.toIterator, sc)           
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
    val matscheme = RandomizedMaterializationScheme2(n_bits, 8, 4, 4)
    val dc = new DataCube(matscheme)
    dc.build(sc)
    dc
 }
  
}


object Schema {
  def load(filename: String): Schema = {
    val file = new File("cubedata/"+filename+"/"+filename+".sch")
    val ois = new ObjectInputStream(new FileInputStream(file))
    val sch = ois.readObject.asInstanceOf[Schema]
    ois.close
    sch
  }
}

