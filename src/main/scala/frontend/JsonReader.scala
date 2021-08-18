//package ch.epfl.data.sudokube
package frontend
import scala.io._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper


object JsonReader {
  private def flatten_record(
    prefix: String,
    o: Object
  ) : Map[String, Object] = {
    
    def p(s: String) = if(prefix == "") s else prefix ++ "." ++ s

    o match {
      case (m: Map[_, _]) => (m.map {
          case (s, o2) => flatten_record(p(s.asInstanceOf[String]),
                                          o2.asInstanceOf[Object])
        }).flatten.toMap

      case (l: List[_]) =>
        l.zipWithIndex.map { case (o2, i) => {
            flatten_record(p(i.toString), o2.asInstanceOf[Object])
        }}.flatten.toMap

      case z => Map((prefix, z))
    }
  }

  private def flt(prefix: String, o: Object) : List[Map[String, Object]] = {

    def p(s: String) = if(prefix == "") s else prefix ++ "." ++ s
    
    o match {
      case (m: Map[_, _]) => (m.map {
          case (s, o2) => flt(p(s.asInstanceOf[String]), 
                               o2.asInstanceOf[Object])
        }).toList.flatten

      case (l: List[_]) =>
        l.asInstanceOf[List[Object]].map(flatten_record(prefix, _))

      case z => List(Map((prefix, z)))
    }
  }

  /** the top level must be of Array type, i.e. the file must start with "[". */
  def read(filename: String) : List[Map[String, Object]] = {
    
    val json = Source.fromFile(filename) 
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val o = mapper.readValue[Object](json.reader())
    flt("", o)
  }
}


