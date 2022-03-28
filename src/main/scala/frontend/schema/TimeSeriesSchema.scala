package frontend
package schema
import frontend.schema.encoders.{ColEncoder, MemCol, NatCol}
import util._
import util.BigBinaryTools._
import breeze.io.CSVReader
import java.io._


class TimeSeriesSchema extends Schema {
    
    implicit val bitPosRegistry = new BitPosRegistry
   
    override def n_bits: Int = bitPosRegistry.n_bits 
    val columns = collection.mutable.Map[String, ColEncoder[_]]()
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
    items.zip(Range(1, items.size+1)).map(l => (encode_tuple(List(("Time", l._2)) ++ l._1.toList), 1L))
  }

    protected def encode_column(key: String, v: Any) = {

     /*  //check if first column
        if(columns.isEmpty) {
            if(v.isInstanceOf[Int]) {
                val vi : Int = v.asInstanceOf[Int]
                val sgn_enc = if(vi < 0) {
                                    val sgn_key = "-" + key
                                    val sgn_c = new NatCol()
                                    columns(sgn_key) = sgn_c
                                    sgn_c.encode_any(1)
                                } 
                                else BigBinary(0)

                val c = new NatCol()
                columns(key) = c
                c.encode_any(math.abs(vi)) + sgn_enc
            }
            else {
                val c = new MemCol[Option[String]](List[Option[String]](None))
                columns(key) = c
                c.encode_any(Some(v))
            }
        }
        else {
           /* val c = ???
            columns(key) = c
            c.encode_any(Some(v))*/
            ???
        }
       */
      ???

    }
  

}

