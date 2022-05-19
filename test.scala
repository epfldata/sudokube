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

object  test {

     def main(args: Array[String]): Unit = { 
    
       

        println("1234567890 : " + validateNumber("1234567890"));  
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


        //System.out.println(isValidURL("htps://www.inazumatv.fr/s2/100.html"))
        genJsonFromStream();

    
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


 def genJsonFromStream(url : String = "https://random-data-api.com/api/name/random_name"): Unit = {

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

            fileWriter.close() 
         }
    }
       
    thread.start()
    println("Enter anything to stop !")
    val a = scala.io.StdIn.readLine
    thread.stopRunning()
  

    }
   
    
}
