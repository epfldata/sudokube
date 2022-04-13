package frontend.schema.encoders

import frontend.schema.{BitPosRegistry, RegisterIdx}
import util.BigBinary

import scala.io.Source
import scala.util.Try

class TypeColEncoder[T](init_size: Int = 1
               ) (implicit bitPosRegistry: BitPosRegistry)  extends DynamicColEncoder[T] {

    var encode_map = new collection.mutable.OpenHashMap[Class[T], Int](math.max(2, init_size))
    var decode_map = new collection.mutable.ArrayBuffer[String](init_size)
    register.registerIdx(init_size)

    def this(init_values: Seq[T])(implicit bitPosRegistry: BitPosRegistry) = {
        this(init_values.size)
        init_values.foreach(x => encode_locally(x.getClass()))
    }
    
    def decode_locally(i: Int): T = decode_map(i).asInstanceOf[T]
    
    override def queries(): Set[Seq[Int]] = Set(Nil, bits)
    def encode_locally(v: T): Int = 
        if (encode_map.isDefinedAt(classOf[v]))
            encode_map(classOf[v])
        else {
            val newpos = encode_map.size
            encode_map += (classOf[v] -> newpos)
            val nameType : String = v match{
                case Class[Int] => "Int"
                case Class[String] => "String"
                case _ => v.toString
            }
            decode_map += nameType
            register.registerIdx(newpos)
            newpos
        }    
    
}