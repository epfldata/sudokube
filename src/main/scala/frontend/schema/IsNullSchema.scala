package frontend
package schema

import frontend.schema.encoders.{ColEncoder, NatCol, IsNullColEncoder}
import util._
import util.BigBinaryTools._
import breeze.io.CSVReader
import java.io._

class IsNullSchema extends TimeSeriesSchema {
  override def n_bits: Int = bitPosRegistry.n_bits

  protected def encode_column(key: String, v: Any) = {

      //check if first column
        if(key == columnTimeLabel) {
          val vi : Int = v.asInstanceOf[Int]
          val c = columns.getOrElse(key, new NatCol())
          columns(key) = c
          c.encode_any(vi)
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
