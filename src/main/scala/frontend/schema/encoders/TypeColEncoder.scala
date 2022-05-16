package frontend.schema.encoders

import frontend.schema.{BitPosRegistry, RegisterIdx}
import util.BigBinary
import java.net.URL
import scala.io.Source
import scala.util.Try
import java.text.SimpleDateFormat
import java.text.ParseException
import java.util.regex.Pattern



class TypeColEncoder[T](init_size: Int = 1
               ) (implicit bitPosRegistry: BitPosRegistry)  extends DynamicColEncoder[T] {

    var encode_map = new collection.mutable.OpenHashMap[String, Int](math.max(2, init_size))
    var decode_map = new collection.mutable.ArrayBuffer[String](init_size)
    register.registerIdx(init_size)
    
    def decode_locally(i: Int): T = decode_map(i).asInstanceOf[T]
    
    override def queries(): Set[Seq[Int]] = Set(Nil, bits)
    def encode_locally(v: T): Int = {
        val s : String = typeToString(v)
        if (encode_map.isDefinedAt(s))
            encode_map(s)
        else {
            val newpos = encode_map.size
            encode_map += (s -> newpos)
            decode_map += s
            register.registerIdx(newpos)
            newpos
        }  
    }
    def typeToString(v: T) : String =
        v match {
          case  _ : Int => "Int"
          case  _ : Long => "Long"
          case  x : Double => "Double"
          case  x : String => {
            if(isDate(x))
                "Date"
            else if(isValidURL(x)) 
                "URL"
            else if(isValidPath(x))
                "file"
            else
                "String"
          }
          case  _ => "Any"

        } 

    def validate(date: String, dateFormat : String) : Boolean = try {
        val df = new SimpleDateFormat(dateFormat)
        //df.setLenient(false)
        df.parse(date)
        true
    } catch {
        case e: ParseException => false
    }

    private def isDate(date : String) : Boolean = {
        if(validate(date, "dd/MM/yyyy") || validate(date, "yyyy/MM/dd") || validate(date, "dd-MM-yyyy") || validate(date, "yyyy-MM-dd"))
            true
        else 
            false
    }

    def isValidURL( urlString : String): Boolean = {
        try {
            var url = new URL(urlString)
            url.toURI();
            return true;
        } catch {
            case _: Throwable => return false
        }
    }
    def isValidPath(pathString : String): Boolean = {
        val r = "(.+(?=\\/))(\\/)(.+(?=\\.))(.*)".r
        return  r.unapplySeq(pathString).isDefined
    }

    def isValidPhoneNumber(numberString : String): Boolean = {

        /* validates US phone numbers
        1234567890  and any 10 digits numbers
        123-456-7890  
        123-456-7890 x1234  
        123-456-7890 ext1234  
        (123)-456-7890  
        123.456.7890  
        123 456 7890  
        //validates phone numbers having 10 digits (9998887776)  
        */
        return "\\d{10}".r.unapplySeq(numberString).isDefined ||
               "\\d{3}[-\\.\\s]\\d{3}[-\\.\\s]\\d{4}".r.unapplySeq(numberString).isDefined ||
               "\\d{4}[-\\.\\s]\\d{3}[-\\.\\s]\\d{3}".r.unapplySeq(numberString).isDefined ||
               "\\d{3}-\\d{3}-\\d{4}\\s(x|(ext))\\d{3,5}".r.unapplySeq(numberString).isDefined ||
               "\\(\\d{3}\\)-\\d{3}-\\d{4}".r.unapplySeq(numberString).isDefined ||
               "\\(\\d{5}\\)-\\d{3}-\\d{3}".r.unapplySeq(numberString).isDefined ||
               "\\(\\d{4}\\)-\\d{3}-\\d{3}".r.unapplySeq(numberString).isDefined 
    }  

    def isValidEmail(str : String): Boolean = {

        val allowedChars  = "abcdefghijklmnopqrstuvwxyz" + "1234567890" + "_-."
        val isAllowed = str.forall(c => allowedChars.contains(c))

        if(isAllowed) {
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

            for(hostName <- emailHostnames) {
                if(str.endsWith("@" + hostName)) {
                    return true
                }
            }
        }
          return false
    }
      
}
