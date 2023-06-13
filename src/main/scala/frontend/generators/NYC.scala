package frontend.generators

import backend.CBackend
import frontend.cubespec.CountMeasure
import frontend.schema.encoders.{LazyMemCol, StaticDateCol, StaticNatCol}
import frontend.schema.{LD2, StaticSchema2}
import util.BigBinary

import java.util.Date
import scala.io.Source

case class NYC()(implicit backend: CBackend) extends StaticCubeGenerator("NYC") {
  override lazy val schemaInstance = schema()
  val measure = new CountMeasure[StaticInput]()

  override def generatePartitions(): IndexedSeq[(Int, Iterator[(BigBinary, Long)])] = {
    val join = (0 until 1000).map { i =>
      val num = String.format("%03d", Int.box(i))
      val n2 = "all.part" + num + ".tsv"
      val size = read(n2).size
      size -> read(n2).map(r => schemaInstance.encode_tuple(r) -> measure.compute(r))
    }
    join
  }

  override def schema(): StaticSchema2 = {

    def uniq(i: Int) = s"tabledata/nyc/uniq/all.$i.uniq"
    import StaticDateCol._
    //val summons_number = LD2[String]("Summons Number", new LazyMemCol(uniq(1)))
    val plate_id = LD2[String]("Plate ID", new LazyMemCol(uniq(2)))
    val registration_state = LD2[String]("Registration State", new LazyMemCol(uniq(3)))
    val plate_type = LD2[String]("Plate Type", new LazyMemCol(uniq(4)))
    val issue_date = LD2[Date]("Issue Date", StaticDateCol.fromFile(uniq(5), simpleDateFormat("MM/dd/yyyy"), true, true, true))
    val violation_code = LD2[String]("Violation Code", new LazyMemCol(uniq(6)))
    val vehicle_body_type = LD2[String]("Vehicle Body Type", new LazyMemCol(uniq(7)))
    val vehicle_make = LD2[String]("Vehicle Make", new LazyMemCol(uniq(8)))
    val issuing_agency = LD2[String]("Issuing Agency", new LazyMemCol(uniq(9)))
    val street_code1 = LD2[String]("Street Code1", new LazyMemCol(uniq(10)))

    val dims0to9 = Vector(plate_id, registration_state, plate_type, issue_date,
      violation_code, vehicle_body_type, vehicle_make, issuing_agency, street_code1)


    val street_code2 = LD2[String]("Street Code2", new LazyMemCol(uniq(11)))
    val street_code3 = LD2[String]("Street Code3", new LazyMemCol(uniq(12)))
    val vehicle_expiration_date = LD2[Date]("Vehicle Expiration Date", StaticDateCol.fromFile(uniq(13), simpleDateFormat("yyyyMMdd"), true, true, true))
    val violation_location = LD2[String]("Violation Location", new LazyMemCol(uniq(14)))
    val violation_precinct = LD2[String]("Violation Precinct", new LazyMemCol(uniq(15)))
    val issuer_precinct = LD2[String]("Issuer Precinct", new LazyMemCol(uniq(16)))
    val issuer_code = LD2[String]("Issuer Code", new LazyMemCol(uniq(17)))
    val issuer_command = LD2[String]("Issuer Command", new LazyMemCol(uniq(18)))
    val issuer_squad = LD2[String]("Issuer Squad", new LazyMemCol(uniq(19)))
    val violation_time = LD2[String]("Violation Time", new LazyMemCol(uniq(20)))

    val dim10to19 = Vector(street_code2, street_code3, vehicle_expiration_date, violation_location, violation_precinct,
      issuer_precinct, issuer_code, issuer_command, issuer_squad, violation_time)
    //using dictionary encoding for violation_time and time_first_observed due to inconsistent format

    val time_first_observed = LD2[String]("Time First Observed", new LazyMemCol(uniq(21)))
    val violation_county = LD2[String]("Violation County", new LazyMemCol(uniq(22)))
    val violation_in_front_of_or = LD2[String]("Violation In Front Of Or Opposite", new LazyMemCol(uniq(23)))
    val house_number = LD2[String]("House Number", new LazyMemCol(uniq(24)))
    val street_name = LD2[String]("Street Name", new LazyMemCol(uniq(25)))
    val intersecting_street = LD2[String]("Intersecting Street", new LazyMemCol(uniq(26)))
    val date_first_observed = LD2[Date]("Date First Observed", StaticDateCol.fromFile(uniq(27), simpleDateFormat("yyyyMMdd"), true, true, true))
    val law_section = LD2[String]("Law Section", new LazyMemCol(uniq(28)))
    val sub_division = LD2[String]("Sub Division", new LazyMemCol(uniq(29)))
    val violation_legal_code = LD2[String]("Violation Legal Code", new LazyMemCol(uniq(30)))

    val dims20to29 = Vector(time_first_observed, violation_county, violation_in_front_of_or, house_number, street_name,
      intersecting_street, date_first_observed, law_section, sub_division, violation_legal_code)


    val days_parking_in_effect = LD2[String]("Days Parking In Effect", new LazyMemCol(uniq(31)))
    val from_hours_in_effect = LD2[String]("From Hours In Effect", new LazyMemCol(uniq(32)))
    val to_hours_in_effect = LD2[String]("To Hours In Effect", new LazyMemCol(uniq(33)))
    val vehicle_color = LD2[String]("Vehicle Color", new LazyMemCol(uniq(34)))
    val unregistered_vehicle = LD2[String]("Unregistered Vehicle?", new LazyMemCol(uniq(35)))
    val vehicle_year = LD2[Date]("Vehicle Year", StaticDateCol.fromFile(uniq(36), simpleDateFormat("yyyy"), true))
    val meter_number = LD2[String]("Meter Number", new LazyMemCol(uniq(37)))
    val feet_from_curb = LD2[Int]("Feet From Curb", StaticNatCol.fromFile(uniq(38)))

    val dims30to39 = Vector(days_parking_in_effect, from_hours_in_effect, to_hours_in_effect, vehicle_color, unregistered_vehicle,
      vehicle_year, meter_number, feet_from_curb) //, violation_post_code, violation_description)

    val allDims = dims0to9 ++ dim10to19 ++ dims20to29 ++ dims30to39 // ++ dims40to42

    val sch = new StaticSchema2(allDims)
    //sch.columnVector.map(c => s"${c.name} has ${c.encoder.bits.size} bits = ${c.encoder.bits}").foreach(println)
    //println("Total = "+sch.n_bits)
    sch
  }

  def read(file: String) = {
    val filename = s"tabledata/nyc/$file"
    val data = Source.fromFile(filename, "utf-8").getLines().map(_.split("\t").tail) //ignore summons_number
    data
  }
}

object NYC {

  def main(args: Array[String]): Unit = {

    val resetSeed = true //for reproducing the same set of materialization decisions
    val seedValue = 0L

    val arg = args.lift(0).getOrElse("all")
    val params = List(
      (15, 18),
      (15, 14), (15, 10), (15, 6),
      (12, 18), (9, 18), (6, 18)
    )
    val maxD = 40

    if ((arg equals "base") || (arg equals "all")) {
      implicit val backend = CBackend.default
      val cg = new NYC()
      if (resetSeed) scala.util.Random.setSeed(seedValue)
      cg.saveBase()
    }

    if ((arg equals "RMS") || (arg equals "all")) {
      implicit val backend = CBackend.default
      val cg = new NYC()
      if (resetSeed) scala.util.Random.setSeed(seedValue)
      params.foreach { case (logN, minD) =>
        cg.saveRMS(logN, minD, maxD)
        backend.reset
      }
    }

    if ((arg equals "SMS") || (arg equals "all")) {
      implicit val backend = CBackend.default
      val cg = new NYC()
      if (resetSeed) scala.util.Random.setSeed(seedValue)
      params.foreach { case (logN, minD) =>
        cg.saveSMS(logN, minD, maxD)
        backend.reset
      }
    }

    if ((arg equals "RMSTrie") || (arg equals "all")) {
      //params.foreach { case (logN, minD) =>
      implicit val backend = CBackend.triestore
      val cg = new NYC()
      val dc = cg.loadRMS(15, 18, maxD)
      dc.loadPrimaryMoments(cg.baseName)
      dc.saveAsTrie(20)
      backend.reset
      //}
    }
    if ((arg equals "SMSTrie") || (arg equals "all")) {
      implicit val backend = CBackend.triestore
      val cg = new NYC()
      val dc = cg.loadSMS(15, 18, maxD)
      dc.loadPrimaryMoments(cg.baseName)
      dc.saveAsTrie(20)
      backend.reset
      //}
    }
  }
}
