package frontend.generators

import breeze.io.CSVReader
import core.DataCube
import frontend.schema.StructuredDynamicSchema

import java.io.FileReader
import java.time.{Instant, LocalDate}
import java.time.temporal.ChronoField._
import scala.util.Random

object Migros {

  def readCSV(name: String, cols: Vector[String]) = {
    val folder = "tabledata/migros"
    val csv = CSVReader.read(new FileReader(s"$folder/${name}.csv"))
    val header = csv.head
    val colIdx = cols.map(c => header.indexOf(c))
    csv.tail.map { r => colIdx.map(i => r(i)) }
  }

  def expdist(n: Int, lambda: Double) = {
    val r = Random.nextDouble()
    val c = 1 - math.exp(-lambda * n)
    (-math.log(1 - r * c) / lambda).toInt
  }

  def logNorm(p1: Double, p2: Double, p3: Double) = {
    val mean = Math.log(p1 + p3)
    val stddev = (Math.log(p2 + p3) - mean) / 4
    (Math.exp(Random.nextGaussian() * stddev + mean) - p3).toInt
    //(Random.nextGaussian() * 10).toInt
  }

  def genNumItems = {
    val mean = 10
    val dev = 5
    var v = Random.nextGaussian() * dev + mean
    while (v <= 1)
      v = Random.nextGaussian() * dev + mean
    v.toInt
  }

  def genHr() = {
    val r = Random.nextDouble()
    val mean = if (r < 0.4) 12 else 18
    val dev = 1.5
    var v = (Random.nextGaussian() * dev + mean).toInt
    while (v <= 6 || v >= 23)
      v = (Random.nextGaussian() * dev + mean).toInt
    v
  }


  def read() = {
    val locs = readCSV("location", Vector("name", "city", "state", "zip_code", "country", "latitude", "longitude"))
    val products = readCSV("product", Vector("PRODUCT_ID", "MANUFACTURER", "DEPARTMENT", "BRAND", "COMMODITY_DESC", "SUB_COMMODITY_DESC"))


    def mk_date(locID: Int, prodID: Int): Instant = ???

    def checkChristmas(dt: Instant, iid: Int) = {
      val desc = products(iid)(4)
      val month = dt.get(MONTH_OF_YEAR)
      desc.startsWith("CHRISTMAS") && month <= 10 && month >= 2
    }

    def mk_prod(locID: Int, dt: Instant): Int = {
      var retry = true
      var v = 0
      while (retry) {
        v = Random.nextInt(products.size)
        retry = false
        retry = retry || checkChristmas(dt, v)
      }
      v
    }

    def mk_loc(prodID: Int, dt: Instant): Int = ???


    val numyears = 1
    val startyear = 1990

    val storeid = expdist(locs.size, 0.001)

    val it = LocalDate.of( startyear + Random.nextInt(numyears), 1, 1).
      `with`(DAY_OF_YEAR, Random.nextInt(365)).
      `with`(HOUR_OF_DAY, genHr()).
      `with`(MINUTE_OF_HOUR, Random.nextInt(60)).
      `with`(SECOND_OF_MINUTE, Random.nextInt(60))
    val weekday = it.get(DAY_OF_WEEK)


    val numItems = genNumItems
    (0 to numItems).map { x =>
      val iid = Random.nextInt(products.size)

    }


  }

  /*
    def save( lrf: Double, lbase: Double) = {
      val (sch, r) = read()
      val rf = math.pow(10, lrf)
      val base = math.pow(10, lbase)
      val dc = new DataCube(OldRandomizedMaterializationStrategy(sch.n_bits, rf, base))
      val name = "brazil"
      sch.save(name)
      dc.build(CBackend.b.mk(sch.n_bits, r.toIterator))
      dc.save2(s"${name}_${lrf}_${lbase}")
    }
  */
  def load(lrf: Double, lbase: Double) = {
    val inputname = "brazil"
    val sch = StructuredDynamicSchema.load(inputname)
    sch.columnVector.map(c => c.name -> c.encoder.bits).foreach(println)
    val dc = DataCube.load(s"${inputname}_${lrf}_${lbase}")
    (sch, dc)
  }

  def queries(sch: StructuredDynamicSchema) = {
    val date = sch.columnVector(2)
    val cust = (3 to 6).map(i => sch.columnVector(i))
    val prod = (7 to 8).map(i => sch.columnVector(i))
    val sel = (9 to 12).map(i => sch.columnVector(i))

    val dateQ = date.encoder.queries
    val custQ = cust.map(_.encoder.queries).reduce(_ union _)
    val prodQ = prod.map(_.encoder.queries).reduce(_ union _)
    val selQ = sel.map(_.encoder.queries).reduce(_ union _)


    println("Date queries = " + dateQ.size)
    println("Cust queries = " + custQ.size)
    println("Product queries = " + prodQ.size)
    println("Seller queries = " + selQ.size)

    dateQ.flatMap { q1 => custQ.flatMap { q2 => prodQ.flatMap { q3 => selQ.map { q4 => q1 ++ q2 ++ q3 ++ q4 } } } }.toList.sortBy(_.length)
  }

  def main(args: Array[String]) = {
    //read()

    val map = collection.mutable.Map[Int, Int]().withDefaultValue(0)
    val N = 100000
    val n = 4000
    val lambda = 0.001
    (0 to N).foreach { x =>
      //val i = logNorm(10.0, 20, 6)
      //val i = genNumItems
      val i = expdist(4000, 0.001)
      map(i) += 1
    }
    val M = map.toList.sortBy(-_._2)

    M.take(10).foreach(println)
    M.takeRight(10).foreach(println)
    //map.toList.sortBy(_._1).foreach(println)
    println(map.size)
    println(map.values.sum)
    println(map.keys.max)
  }

}
