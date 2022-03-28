package frontend
package schema
import frontend.schema.encoders.{ColEncoder, MemCol, NatCol, IsNullColEncoder}
import util._
import util.BigBinaryTools._
import breeze.io.CSVReader
import java.io._


class TimeSeriesSchema extends Schema {
    
    override def n_bits: Int = bitPosRegistry.n_bits

    implicit val bitPosRegistry = new BitPosRegistry
    val columns = collection.mutable.Map[String, ColEncoder[_]]()
    val columnTimeLabel : String = "Time"
    def columnList = columns.toList

    override  def read(filename: String, measure_key: Option[String] = None, map_value : Object => Long = _.asInstanceOf[Long]
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
    items.zip(Range(1, items.size+1)).map(l => (encode_tuple(List((columnTimeLabel, l._2)) ++ l._1.toList), 1L))
  }

    protected def encode_column(key: String, v: Any) = {

      //check if first column
        if(key == columnTimeLabel) {
            if(v.isInstanceOf[Int]) {
                val vi : Int = v.asInstanceOf[Int]
                val sgn_enc = if(vi < 0) {
                                    val sgn_key = "-" + key
                                    val sgn_c = columns.getOrElse(key, new NatCol())
                                    columns(sgn_key) = sgn_c
                                    sgn_c.encode_any(1)
                                } 
                                else BigBinary(0)

                val c = columns.getOrElse(key, new NatCol())
                columns(key) = c
                c.encode_any(math.abs(vi)) + sgn_enc
            }
            else {
                val c = columns.getOrElse(key, new MemCol[Option[String]](List[Option[String]](None)))
                columns(key) = c
                c.encode_any(Some(v))
            }
        }
        else {
           val c = if(v.isInstanceOf[Int]) {
           columns.getOrElse(key, new IsNullColEncoder[Option[Int]])
           } else columns.getOrElse(key, new IsNullColEncoder[Option[String]])
            columns(key) = c
            c.encode_any(Some(v))
        }

    }
  

}

