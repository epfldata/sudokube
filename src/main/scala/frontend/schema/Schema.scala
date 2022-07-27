//package ch.epfl.data.sudokube
package frontend
package schema

import backend._
import breeze.io.CSVReader
import core._
import core.materialization.RandomizedMaterializationStrategy
import frontend.schema.encoders.ColEncoder
import util.BigBinaryTools._
import util._

import java.io._
import java.net.{HttpURLConnection, URL}

@SerialVersionUID(6L)
trait Schema extends Serializable {
  // abstract members
  def n_bits: Int

  def columnList: IndexedSeq[(String, ColEncoder[_])]
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

    if (measure_key.isEmpty) {
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
  def save(filename: String): Unit = {
    val file = new File("cubedata/"+filename+"/"+filename+".sch")
    if(!file.exists())
      file.getParentFile.mkdirs()
    val oos = new ObjectOutputStream(new FileOutputStream(file))
    oos.writeObject(this)
    oos.close()
  }

  def decode_dim(q_bits: IndexedSeq[Int]): Seq[Seq[String]] = {
    val relevant_cols = columnList.filter(_._2.bits.intersect(q_bits).nonEmpty)
    val universe = relevant_cols.flatMap {
      case (_, c) => c.bits
    }.sorted

    BitUtils.group_values(q_bits, universe).map(
      x => relevant_cols.map {
        case (key, c) => {
          val l = x.flatMap(y =>
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
            }).toSet.toList.sorted // duplicate elimination

          if (l.length == 1) key + "=" + l.head
          else key + " in " + l
        }
      })
  }


  /**
   * Reads potentially an infinite amount of data from a stream given by the url argument.
   * Stopping the reading of the stream is possible by entering any character in the command line.
   * While a thread reads the data from the stream and places them in one of the two buffers (temporary files),
   * another thread will read the other buffer (in parallel) and update the cuboid.
   * The size of the buffers is defined by the bufferSize argument and it is possible to delay each reading of the stream, using the delay argument (in milliseconds).
   */
  def readFromStream(measure_key: Option[String] = None, map_value : Object => Long = _.asInstanceOf[Long], url : String, bufferSize : Int = 7, delay : Int = 0): DataCube = {

     ///Initiates the partial cuboid
     @volatile var sc = CBackend.b.initPartial()

      val threadWrite  = new Thread {

         @volatile private var end = false

         def stopRunning(): Unit = end = true

         override def run {

            val pathTemp1 : String = "temp1.json"
            val pathTemp2 : String  = "temp2.json"
            //the file path where the data read from the stream will be placed.
            var pathWrite : String = pathTemp1
            //the path of the file from which the data will be taken to update the cuboid.
            var pathRead : String = pathTemp2
            val testFileWriter = new BufferedWriter(new FileWriter(new File("total.json")));

            //Swaps the two buffers
            def swapPaths(): Unit = {
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

            //This function is used by threadWrite and reads the bufferSize data from the stream given by url and puts it in the file path, pathWrite under the json array format.
            def getJsonArray(): Unit = {
              val fileWriter = new BufferedWriter(new FileWriter(new File(pathWrite)));
              fileWriter.write("[")
              for(i <- 1 to bufferSize) {
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
                          testFileWriter.write(line)
                          line = bufferedReader.readLine()
                        }
                         testFileWriter.write("\n")
                        if(i == bufferSize) {
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

                  if(delay > 0) {
                    try {
                      Thread.sleep(delay)
                    }
                    catch {
                       case _: Throwable => println("error while trynig to delay the http request")
                    }
                  }
              }
              fileWriter.close()
            }

            // this thread reads from the file at pathRead and updates the partial cuboid using the mkPartial function
            def getThreadRead(): Thread = {
                new Thread {
                    override def run {
                        var r = read(pathRead, measure_key, map_value)
                        sc = CBackend.b.addPartial(n_bits, r.toIterator, sc)
                    }
                }
            }

            var threadRead = getThreadRead()
            getJsonArray()
            swapPaths()

            //The main loop that reads from one buffer and writes to the other at the same time.
            while(!end) {
              threadRead.start()
              getJsonArray()
              threadRead.join()
              swapPaths()
              threadRead = getThreadRead()
            }
            testFileWriter.close()
            threadRead.start()
            deleteFile(pathWrite)
            threadRead.join()
            deleteFile(pathRead)
          }
     }

    threadWrite.start()
    //Demands the user to enter any character to stop reading the stream.
    println("Reading from a Stream... (Enter anything to stop)")
    scala.io.StdIn.readLine()
    //Waits for the iteration of the main loop to end and stops threadWrite.
    threadWrite.stopRunning()
    threadWrite.join()
    val  m = new RandomizedMaterializationStrategy(n_bits, 8, 4)
    val dc = new DataCube()
    //converts the partial cuboid into a cuboid
    sc = CBackend.b.finalisePartial(sc)
    dc.build(sc,  m)
    dc
 }

}


object Schema {
  def load(filename: String): Schema = {
    val file = new File("cubedata/"+filename+"/"+filename+".sch")
    val ois = new ObjectInputStream(new FileInputStream(file)){
      override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
        try { Class.forName(desc.getName, false, getClass.getClassLoader) }
        catch { case ex: ClassNotFoundException => super.resolveClass(desc) }
      }
    }
    val sch = ois.readObject.asInstanceOf[Schema]
    ois.close()
    sch
  }
}

