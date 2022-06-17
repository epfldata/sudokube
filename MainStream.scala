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
import frontend.JsonReader
import java.util.concurrent.TimeUnit
import frontend._
import backend._
import core._
import util.BigBinary

object MainStream {

      def main(args: Array[String]): Unit = { 
        val sch = new schema.DynamicSchema
        val dc = sch.readFromStream(url = "https://random-data-api.com/api/crypto_coin/random_crypto_coin")
        //val qH = sch.columns("id").bits.toList
        val qH = List()
        val qV = sch.columns("coin_name").bits.toList
        val q = (qV ++ qH).sorted
        val r = dc.naive_eval(q)
        println("r =" + r.mkString(" "))
       // val od = OnlineDisplay(sch, dc, PrettyPrinter.formatPivotTable(sch, qH, qV)) //FIXME: Fixed qV and qH. Make variable depending on query
        //od.l_run(q, 2)

      }
}
