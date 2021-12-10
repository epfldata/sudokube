package frontend.generators

import frontend.schema.encoders.{DateCol, MemCol, NatCol}
import frontend.schema.{BitPosRegistry, LD2, StructuredDynamicSchema}
import util.BigBinary

import java.text.SimpleDateFormat
import java.util.Date
import scala.concurrent.ExecutionContext
import scala.io.Source
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.{Duration => ScalaDur}

object NYC extends CubeGenerator("NYC") {

  val c4MinYear = 1970
  val c4MaxYear = 2069
  val c12MinYear = 1970
  val c12MaxYear = 2069
  val c26MinYear = 1970
  val c26MaxYear = 2033
  val c35MinYear = 1970
  val c35MaxYear = 2069

  override def generate(): (StructuredDynamicSchema, Seq[(BigBinary, Long)]) = {
    implicit val ec = ExecutionContext.global
    val join = (0 until 1000).map(i => Future {
      val num =  String.format("%03d",Int.box(i))
      val n2 = "all.part" + num + ".tsv"
      val res = read(n2)
      print(" R" + i)
      res
    })

    val NCols = 38
    implicit val bpr = new BitPosRegistry

    //def getDistinct[T](id: Int) = {
    // val futdists = join.map { fvs =>
    //     fvs.map { vs =>
    //      vs.map{r =>
    //        r(id).asInstanceOf[T]}.distinct
    //    }
    //  }
    //  val f  = Future.sequence(futdists).map(_.flatten.distinct)
    // val res = Await.result(f, ScalaDur.Inf)
    //  println("Distinct "+id)
    //  res
    //}

    val distArray = new Array[Seq[String]](NCols)
    val distFutures = (0 until NCols).map { i =>
      Future {
        val filename = s"tabledata/nyc/cols/col${i + 1}"
        distArray(i) = Source.fromFile(filename).getLines().toSeq
        print(" D" + i)
      }
    }

    distFutures.foreach(f => Await.result(f, ScalaDur.Inf)) //block till all distinct values have been read
    println("\n All distinct loaded")
    def getDistinct(i: Int) = distArray(i)

    val summons_distinct = getDistinct(0)
    val summons_number = LD2[String]("Summons Number", new MemCol(summons_distinct))

    val plate_id_dist = getDistinct(1)
    val plate_id = LD2[String]("Plate ID", new MemCol(plate_id_dist))

    val registration_state_dist = getDistinct(2)
    val registration_state = LD2[String]("Registration State", new MemCol(registration_state_dist))

    val d3 = getDistinct(3)
    val plate_type = LD2[String]("Plate Type", new MemCol(d3))


    val issue_date = LD2[Date]("Issue Date", new DateCol(c4MinYear, c4MaxYear, true, true))

    val d5 = getDistinct(5)
    val violation_code = LD2[String]("Violation Code", new MemCol(d5))

    val d6 = getDistinct(6)
    val vehicle_body_type = LD2[String]("Vehicle Body Type", new MemCol(d6))

    val d7 = getDistinct(7)
    val vehicle_make = LD2[String]("Vehicle Make", new MemCol(d7))

    val d8 = getDistinct(8)
    val issuing_agency = LD2[String]("Issuing Agency", new MemCol(d8))

    val d9 = getDistinct(9)
    val street_code1 = LD2[String]("Street Code1", new MemCol(d9))

    val dims0to9 = Vector(summons_number, plate_id, registration_state, plate_type, issue_date,
      violation_code, vehicle_body_type, vehicle_make, issuing_agency, street_code1)


    val d10 = getDistinct(10)

    val street_code2 = LD2[String]("Street Code2", new MemCol(d10))
    val d11 = getDistinct(11)
    val street_code3 = LD2[String]("Street Code3", new MemCol(d11))


    val vehicle_expiration_date = LD2[Date]("Vehicle Expiration Date", new DateCol(c12MinYear, c12MaxYear, true, true))

    val d13 = getDistinct(13)
    val violation_location = LD2[String]("Violation Location", new MemCol(d13))

    val d14 = getDistinct(14)
    val violation_precinct = LD2[String]("Violation Precinct", new MemCol(d14))

    val d15 = getDistinct(15)
    val issuer_precinct = LD2[String]("Issuer Precinct", new MemCol(d15))

    val d16 = getDistinct(16)
    val issuer_code = LD2[String]("Issuer Code", new MemCol(d16))

    val d17 = getDistinct(17)
    val issuer_command = LD2[String]("Issuer Command", new MemCol(d17))


    val d18 = getDistinct(18)
    val issuer_squad = LD2[String]("Issuer Squad", new MemCol[String](d18))

    val d19 = getDistinct(19)
    val violation_time = LD2[String]("Violation Time", new MemCol(d19))

    val dim10to19 = Vector(street_code2, street_code3, vehicle_expiration_date, violation_location, violation_precinct,
      issuer_precinct, issuer_code, issuer_command, issuer_squad, violation_time)

    val d20 = getDistinct(20)
    val time_first_observed = LD2[String]("Time First Observed", new MemCol(d20))

    val d21 = getDistinct(21)
    val violation_county = LD2[String]("Violation County", new MemCol[String](d21))

    val d22 = getDistinct(22)
    val violation_in_front_of_or = LD2[String]("Violation In Front Of Or Opposite", new MemCol(d22))

    val d23 = getDistinct(23)
    val house_number = LD2[String]("House Number", new MemCol(d23))

    val d24 = getDistinct(24)
    val street_name = LD2[String]("Street Name", new MemCol(d24))

    val d25 = getDistinct(25)
    val intersecting_street = LD2[String]("Intersecting Street", new MemCol(d25))


    val date_first_observed = LD2[Date]("Date First Observed", new DateCol(c26MinYear, c26MaxYear, true, true))

    val d27 = getDistinct(27)
    val law_section = LD2[String]("Law Section", new MemCol(d27))

    val d28 = getDistinct(28)
    val sub_division = LD2[String]("Sub Division", new MemCol(d28))

    val d29 = getDistinct(29)
    val violation_legal_code = LD2[String]("Violation Legal Code", new MemCol(d29))

    val dims20to29 = Vector(time_first_observed, violation_county, violation_in_front_of_or, house_number, street_name,
      intersecting_street, date_first_observed, law_section, sub_division, violation_legal_code)

    val d30 = getDistinct(30)
    val days_parking_in_effect = LD2[String]("Days Parking In Effect", new MemCol(d30))

    val d31 = getDistinct(31)
    val from_hours_in_effect = LD2[String]("From Hours In Effect", new MemCol(d31))

    val d32 = getDistinct(32)
    val to_hours_in_effect = LD2[String]("To Hours In Effect", new MemCol(d32))

    val d33 = getDistinct(33)
    val vehicle_color = LD2[String]("Vehicle Color", new MemCol(d33))

    val d34 = getDistinct(34)
    val unregistered_vehicle = LD2[String]("Unregistered Vehicle?", new MemCol(d34))

    val vehicle_year = LD2[Date]("Vehicle Year", new DateCol(c35MinYear, c35MaxYear, true, true))

    val d36 = getDistinct(36)
    val meter_number = LD2[String]("Meter Number", new MemCol(d36))


    val feet_from_curb = LD2[Int]("Feet From Curb", new NatCol(100))

    /*
    val d38 = getDistinct(38)
    val violation_post_code = LD2[String]("Violation Post Code", new MemCol(d38))

    val d39 = getDistinct(39)
    val violation_description = LD2[String]("Violation Description", new MemCol(d39))
*/
    val dims30to39 = Vector(days_parking_in_effect, from_hours_in_effect, to_hours_in_effect, vehicle_color, unregistered_vehicle,
      vehicle_year, meter_number, feet_from_curb) //, violation_post_code, violation_description)
    /*
        val d40 = getDistinct(40)
        val no_standing_or_stopping = LD2[String]("No Standing or Stopping Violation", new MemCol(d40))

        val d41 = getDistinct(41)
        val hydrant_violation = LD2[String]("Hydrant Violation", new MemCol(d41))

        val d42 = getDistinct(42)
        val double_parking_violation = LD2[String]("Double Parking Violation", new MemCol(d42))

        val dims40to42 = Vector(no_standing_or_stopping, hydrant_violation, double_parking_violation)
        */

    val allDims = dims0to9 ++ dim10to19 ++ dims20to29 ++ dims30to39 // ++ dims40to42
    assert(allDims.size == NCols)
    val sch = new StructuredDynamicSchema(allDims)
    val R = {
      val futlists = join.indices.map { partid =>
        join(partid).map { fkeys =>
          val res = fkeys.map { k =>
            sch.encode_tuple(k) -> 1L
          }
          print(" E" + partid)
          res
        }

      }

      val f = Future.sequence(futlists).map(_.flatten)
      Await.result(f, ScalaDur.Inf)
    }
    println("\n Encoding complete")
    (sch, R)
  }

  def read(file: String) = {
    val filename = s"tabledata/nyc/$file"
    val data = Source.fromFile(filename, "utf-8").getLines().map(_.split("\t"))
    val f1 = new SimpleDateFormat("MM/dd/yyyy")
    val f2 = new SimpleDateFormat("yyyyMMdd")
    val f3 = new SimpleDateFormat("hhmma")

    val join = data.drop(1).flatMap { row =>
      if (row.size <= 37)
        None
      else {
        //assert(row.size >= 37, s"Row size is ${row.size} in year $year for row <${row.mkString("  ")}>")
        val key = row.indices.map {
          case 4 =>
            val c4 = row(4).take(10) //issue_date
            try {
              if (c4.isEmpty || c4.equals("0"))
                new Date(c4MinYear-1900, 0, 1)
              else
                f1.parse(c4)
            } catch {
              case e: Exception => new Date(c4MinYear-1900, 0, 1)
            }

          case 12 =>
            val c12 = row(12)  //vehicle_expiry_date
            try {
              if (c12.isEmpty || c12.equals("0") || c12.startsWith("01/") || c12.contains("88"))
                new Date(c12MinYear-1900, 0, 1)
              else
                f2.parse(c12)
            } catch {
              case e: Exception => new Date(c12MinYear-1900, 0, 1)
            }

          //case 19 =>
          //  val c19 = row(19)  //violation_time
          //  try {
          //    if (c19.isEmpty)
          //      new Date(0, 0, 1)
          //    else if (c19.endsWith("A") || c19.endsWith("P"))
          //      f3.parse(c19 + "M")
          //    else
          //      f3.parse(c19 + "AM")
          //  } catch {
          //    case e: Exception => new Date(0, 0, 1)
          //  }
          //
          //case 20 =>
          //  val c20 = row(20) //time_first observed
          //  try {
          //    if (c20.isEmpty)
          //      new Date(0, 0, 1)
          //    else if (c20.endsWith("A") || c20.endsWith("P"))
          //      f3.parse(c20 + "M")
          //    else f3.parse(c20 + "AM")
          //  } catch {
          //    case e: Exception => new Date(0, 0, 1)
          //  }

          case 26 =>
            val c26 = row(26) //date first observed
            try {
              if (c26.isEmpty || c26.equals("0") || c26.startsWith("01/"))
                new Date(c26MinYear-1900, 0, 1)
              else
                f2.parse(c26)
            } catch {
              case e: Exception => new Date(c26MinYear-1900, 0, 1)
            }

          case 35 =>
            val c35 = row(35)
            if(c35.isEmpty || c35.equals("0"))
              new Date(c35MinYear-1900, 0, 1)
            else {
              try {
                new Date(c35.toInt-1900, 0, 1)
              } catch {
                case e: Exception => new Date(c35MinYear-1900, 0, 1)
              }

            }
          case i => row(i)
        }
        Some(key)
      }
    }.toVector
    join
  }

  def main(args: Array[String]): Unit = {
    //saveBase()
    buildFromBase(-80.6, 0.19)

  }
}
