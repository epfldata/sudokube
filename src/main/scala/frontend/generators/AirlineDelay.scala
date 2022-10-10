package frontend.generators

import backend.CBackend
import frontend.schema.{BD2, Dim2, LD2, Schema2, StaticSchema2}
import util.BigBinary
import frontend.schema.encoders.{LazyMemCol, StaticDateCol, StaticNatCol}
import com.github.tototoshi.csv._

import java.util.Date
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source

class AirlineDelay(implicit backend: CBackend) extends CubeGenerator("AirlineDelay") {
  override def generatePartitions(): IndexedSeq[(Int, Iterator[(BigBinary, Long)])] = {
    implicit val ec = ExecutionContext.global
    val join = (0 until 1000).map { i =>
      Future {
        println(s"Start Reading partition ${i} for size")
        val num = String.format("%03d", Int.box(i))
        val n2 = "airline.part" + num + ".csv"

        val size = read(n2).size
        val it = read(n2).map(r => schemaInstance.encode_tuple(r) -> 1L)
        println(s"Finish Reading partition $i for size")
        size -> it
      }
    }
    Await.result(Future.sequence(join), Duration.Inf)
  }
  def read(file: String) = {
    val filename = s"tabledata/airline/$file"
    val data = CSVReader.open(filename).iterator.map{s =>
      val sIdx  = s.toIndexedSeq
      notskipped.map(i => sIdx(i))
    }
    data
  }
  val skipped = Set[Int](5, 32, 33, 34, 35, 43, 44, 45, 46)
  val notskipped = (0 to 92 ).toSet.diff(skipped).toVector.sorted
  override protected def schema(): Schema2 = {
    def uniq(i: Int) = s"tabledata/airline/uniq/col$i.uniq"
    val alldims = collection.mutable.ArrayBuffer[Dim2]()
    val floatToInt = StaticNatCol.floatToInt(0)(_)
    val year = LD2[Int]("Year", StaticNatCol.fromFile(uniq(0)))
    val quarter = LD2[Int]("Quarter", StaticNatCol.fromFile(uniq(1)))
    val month = LD2[Int]("Month", StaticNatCol.fromFile(uniq(2)))
    val dayofmonth = LD2[Int]("DayOfMonth", StaticNatCol.fromFile(uniq(3)))
    val dayofweek = LD2[Int]("DayOfWeek", StaticNatCol.fromFile(uniq(4)))
    alldims += BD2("Time", Vector(year, quarter, month, dayofmonth, dayofweek), true)
    //skip flight date

    val reportingAirline = LD2[String]("ReportingAirline", new LazyMemCol(uniq(6)))
    val dotID = LD2[String]("ReportingAirline_DOT_ID", new LazyMemCol(uniq(7)))
    val iata_reportingAirline = LD2[String] ("IATA_ReportingAirline", new LazyMemCol(uniq(8)))
    val tailnumber = LD2[String]("TailNumber", new LazyMemCol(uniq(9)))
    val fligtnumberRA = LD2[Int]("FlightNumber", StaticNatCol.fromFile(uniq(10)))
    alldims += BD2("FlightDetails", Vector(reportingAirline, dotID, iata_reportingAirline, tailnumber, fligtnumberRA), false)

    val originAirportID = LD2[String]("OriginAirportID", new LazyMemCol(uniq(11)))
    val originAirportSeqID = LD2[String]("OriginAirportSeqID", new LazyMemCol(uniq(12)))
    val originCityMarketID = LD2[String]("OriginCityMarketID", new LazyMemCol(uniq(13)))
    val origin = LD2[String]("Origin", new LazyMemCol(uniq(14)))
    val originCityName = LD2[String]("OriginCityName", new LazyMemCol(uniq(15)))
    val originState = LD2[String]("OriginState", new LazyMemCol(uniq(16)))
    val originStateFips = LD2[String]("OriginStateFIPS", new LazyMemCol(uniq(17)))
    val originStateName = LD2[String]("OriginStateName", new LazyMemCol(uniq(18)))
    val originWAC = LD2[String]("OriginWAC", new LazyMemCol(uniq(19)))
    alldims += BD2("OriginAirport", Vector(originAirportID, originAirportSeqID, originCityMarketID, origin, originCityName, originState, originStateFips, originStateName, originWAC), false)

    val destAirportID = LD2[String]("DestinationAirportID", new LazyMemCol(uniq(20)))
    val destAirportSeqID = LD2[String]("DestinationAirportSeqID", new LazyMemCol(uniq(21)))
    val destCityMarketID = LD2[String]("DestinationCityMarketID", new LazyMemCol(uniq(22)))
    val destination = LD2[String]("Destination", new LazyMemCol(uniq(23)))
    val destCityName = LD2[String]("DestinationCityName", new LazyMemCol(uniq(24)))
    val destState = LD2[String]("DestinationState", new LazyMemCol(uniq(25)))
    val destStateFips = LD2[String]("DestinationStateFIPS", new LazyMemCol(uniq(26)))
    val destinationStateName = LD2[String]("DestinationStateName", new LazyMemCol(uniq(27)))
    val destWAC = LD2[String]("DestinationWAC", new LazyMemCol(uniq(28)))
    alldims += BD2("DestinationAirport", Vector(destAirportID, destAirportSeqID, destCityMarketID, destination, destCityName, destState, destStateFips, destinationStateName, destWAC), false)

    import StaticDateCol.simpleDateFormat
    val crsDepTime = LD2[Date]("CRSDepTime", StaticDateCol.fromFile(uniq(29), simpleDateFormat("HHmm"), hasHr = true, hasMin = true))
    val depTime = LD2[Date]("DepTime", StaticDateCol.fromFile(uniq(30),simpleDateFormat("HHmm"), hasHr = true, hasMin = true))
    val depDifference = LD2[Int]("DepartureDifference", StaticNatCol.fromFile(uniq(31), floatToInt))
    //skip delay
    //skip delay15
    //skip delaygroups
    //skip deptimeblk
    alldims += BD2("DepartureTime", Vector(crsDepTime, depTime, depDifference), true)


    val taxiOut = LD2[Int]("TaxiOut", StaticNatCol.fromFile(uniq(36), floatToInt))
    val wheelsOff = LD2[Date]("WheelsOff", StaticDateCol.fromFile(uniq(37), simpleDateFormat("HHmm"), hasHr = true, hasMin = true))
    val wheelsOn = LD2[Date]("WheelsOn", StaticDateCol.fromFile(uniq(38), simpleDateFormat("HHmm"), hasHr = true, hasMin = true))
    val taxiIn = LD2[Int]("TaxiIn", StaticNatCol.fromFile(uniq(39), floatToInt))
    alldims += BD2("Taxi", Vector(taxiOut, wheelsOff, wheelsOn, taxiIn), true)

    val crsArrTime = LD2[Date]("CRSArrTime", StaticDateCol.fromFile(uniq(40), simpleDateFormat("HHmm"), hasHr = true, hasMin = true))
    val arrTime = LD2[Date]("ArrTime", StaticDateCol.fromFile(uniq(41), simpleDateFormat("HHmm"), hasHr = true, hasMin = true))
    val arrDiff = LD2[Int]("ArrivalDifference", StaticNatCol.fromFile(uniq(42), floatToInt))
    //delay minutes
    //del15
    //delaygroup
    //arrivaltimeblk
    alldims += BD2("ArrivalTime", Vector(crsArrTime, arrTime, arrDiff), true)

    val cancelled = LD2[String]("Cancelled", new LazyMemCol(uniq(47)))
    val cancellationCode = LD2[String]("CancellationCode", new LazyMemCol(uniq(48)))
    val diverted = LD2[String]("Diverted", new LazyMemCol(uniq(49)))
    alldims += BD2("FlightStatus", Vector(cancelled, cancellationCode, diverted), true)

    val crsElapsedTime = LD2[Int]("CRSElapsedTime", StaticNatCol.fromFile(uniq(50), floatToInt))
    val actualElapsedTime = LD2[Int]("ActualElapsedTime", StaticNatCol.fromFile(uniq(51), floatToInt))
    val airTime = LD2[Int]("AirTime", StaticNatCol.fromFile(uniq(52), floatToInt))
    alldims += BD2("ElapsedTime", Vector(crsElapsedTime, actualElapsedTime, airTime), true)

    val flights = LD2[String]("Flights", new LazyMemCol(uniq(53)))
    alldims += flights

    val distance = LD2[Int]("Distance", StaticNatCol.fromFile(uniq(54), floatToInt))
    val distanceGroup = LD2[Int]("DistanceGroup", StaticNatCol.fromFile(uniq(55)))
    alldims += BD2("Distance", Vector(distance, distanceGroup), false)

    val carrierDelay = LD2[Int]("CarrierDelay", StaticNatCol.fromFile(uniq(56), floatToInt))
    val weatherDelay = LD2[Int]("WeatherDelay", StaticNatCol.fromFile(uniq(57), floatToInt))
    val nasDelay = LD2[Int]("NASDelay", StaticNatCol.fromFile(uniq(58), floatToInt))
    val securityDelay = LD2[Int]("SecurityDelay", StaticNatCol.fromFile(uniq(59), floatToInt))
    val lateAircraftDelay = LD2[Int]("LateAircraftDelay", StaticNatCol.fromFile(uniq(60), floatToInt))
    alldims += BD2("Delays", Vector(carrierDelay, weatherDelay, nasDelay, securityDelay, lateAircraftDelay), true)

    val firstDepTime = LD2[Date]("FirstDepTime", StaticDateCol.fromFile(uniq(61), simpleDateFormat("HHmm"), hasHr = true, hasMin = true))
    val totaladdGtime = LD2[Int]("TotalAddGTime", StaticNatCol.fromFile(uniq(62), floatToInt))
    val longestaddGtime = LD2[Int]("LongestAddGTime", StaticNatCol.fromFile(uniq(63), floatToInt))
    alldims += BD2("FirstDeparture", Vector(firstDepTime, totaladdGtime, longestaddGtime), true)

    val divAirportLandings = LD2[Int]("DivAirportLandings", StaticNatCol.fromFile(uniq(64)))
    val divReachedDest = LD2[String]("DivReachedDest", new LazyMemCol(uniq(65)))
    val divActualElapsedTime= LD2[Int]("DivActualElapsedTime", StaticNatCol.fromFile(uniq(66), floatToInt))
    val divArrivalDelay = LD2[Int]("DivArrivalDelay", StaticNatCol.fromFile(uniq(67), floatToInt))
    val divDistance = LD2[Int]("DivDistance", StaticNatCol.fromFile(uniq(68), floatToInt))
    alldims += BD2("Diversions", Vector(divAirportLandings, divReachedDest, divActualElapsedTime, divArrivalDelay, divDistance), true)
    def divairport(num: Int) = {
      val startId = 69 + (num-1) * 8
      val divXAiport = LD2[String](s"Div${num}Airport", new LazyMemCol(uniq(startId)))
      val divXAiportID = LD2[String](s"Div${num}AirportID", new LazyMemCol(uniq(startId + 1)))
      val divXAiportSeqID = LD2[String](s"Div${num}AirportSeqID", new LazyMemCol(uniq(startId + 2)))
      val divXWheelsOn = LD2[Date](s"Div${num}WheelsOn",  StaticDateCol.fromFile(uniq(startId + 3), simpleDateFormat("HHmm"), hasHr = true, hasMin = true))
      val divXTotalGTime = LD2[Int](s"Div${num}TotalGTime", StaticNatCol.fromFile(uniq(startId + 4), floatToInt))
      val divXLongestGTime = LD2[Int](s"Div${num}LongestGTime", StaticNatCol.fromFile(uniq(startId + 5), floatToInt))
      val divXWheelsOff = LD2[Date](s"Div${num}WheelsOff", StaticDateCol.fromFile(uniq(startId + 6), simpleDateFormat("HHmm"), hasHr = true, hasMin = true))
      val divXTailNum = LD2[String](s"Div${num}TailNum", new LazyMemCol(uniq(startId + 7)))
      val dims = Vector(divXAiport,divXAiportID,divXAiportSeqID,divXWheelsOn,divXTotalGTime,divXLongestGTime,divXWheelsOff,divXTailNum)
      BD2(s"Div$num", dims, true)
    }
    alldims += divairport(1)
    alldims += divairport(2)
    alldims += divairport(3)
    //alldims += divairport(4)
    //alldims += divairport(5)
    val sch = new StaticSchema2(alldims.toVector)
    //sch.columnVector.map(c => s"${c.name} has ${c.encoder.bits.size} bits = ${c.encoder.bits}").foreach(println)
    //println("Total = "+sch.n_bits)
    sch
  }
}


object AirlineDelay {
  implicit val backend = CBackend.default
  def main(args: Array[String]): Unit = {
    println("Loading Schema")
    val cg = new AirlineDelay()

    val resetSeed = true
    val seedValue = 0L
    val maxD = 30
    val params = List(
      (17, 10), (13, 10),
      (15, 6), (15, 10)// (15, 14)
    )
    if(resetSeed) scala.util.Random.setSeed(seedValue)
    //cg.saveBase()
    params.foreach { case (logN, minD) =>
      cg.saveRMS(logN, minD, maxD)
    cg.saveSMS(logN, minD, maxD)
    }
  }
}

