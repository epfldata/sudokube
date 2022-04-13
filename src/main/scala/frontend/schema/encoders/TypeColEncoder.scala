package frontend.schema.encoders

import frontend.schema.{BitPosRegistry, RegisterIdx}
import util.BigBinary

import scala.io.Source
import scala.util.Try
import java.text.SimpleDateFormat
import java.text.ParseException

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

}