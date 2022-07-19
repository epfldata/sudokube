package frontend.generators

import breeze.io.CSVReader
import core.PartialDataCube
import core.materialization.{RandomizedMaterializationScheme, SchemaBasedMaterializationScheme}
import frontend.schema.encoders.{LazyMemCol, StaticDateCol, StaticNatCol}
import frontend.schema.{BD2, LD2, Schema2, StaticSchema2}
import util.Profiler

import java.io.FileReader
import java.util.Date
import scala.concurrent.ExecutionContext

case class SSB(sf: Int) extends CubeGenerator(s"SSB-sf$sf") {
  val folder = s"tabledata/SSB/sf${sf}"

  override def schema(): Schema2 = {
    def uniq(table: String)(i: Int) = s"$folder/uniq/$table.$i.uniq"
    import StaticDateCol._
    val louniqs = uniq("lineorder") _
    //val oidCol = LD2[String]("order_key", new LazyMemCol(louniqs(1)))
    //val lnCol = LD2[Int]("line_number", StaticNatCol.fromFile(louniqs(2)))
    //3 -> custkey
    //4 -> partkey
    //5 -> suppkey
    val orderDateCol = LD2[Date]("order_date", StaticDateCol.fromFile(louniqs(6),simpleDateFormat("yyyyMMdd"), true, true, true))
    val oprioCol = LD2[String]("ord_priority", new LazyMemCol(louniqs(7)))
    val shiprioCol = LD2[String]("ship_priority", new LazyMemCol(louniqs(8)))
    val qtyCol = LD2[Int]("quantity", StaticNatCol.fromFile(louniqs(9)))
    val expriceCol = LD2[Int]("extended_price", StaticNatCol.fromFile(louniqs(10)))
    //val totalpriceCol = LD2[Int]("total_price", StaticNatCol.fromFile(louniqs(11)))
    val discountCol = LD2[Int]("discount", StaticNatCol.fromFile(louniqs(12)))
    val revenueCol = LD2[Int]("revenue", StaticNatCol.fromFile(louniqs(13)))
    val supcostCol = LD2[Int]("sup_cost", StaticNatCol.fromFile(louniqs(14)))
    val taxCol = LD2[Int]("tax", StaticNatCol.fromFile(louniqs(15)))
    val commitdateCol = LD2[Date]("commit_date", StaticDateCol.fromFile(louniqs(16), simpleDateFormat("yyyyMMdd"), true, true, true))
    val shipmodCol = LD2[String]("ship_mode", new LazyMemCol(louniqs(17)))

    val ordDims = BD2("Order", Vector(orderDateCol, oprioCol, shiprioCol, qtyCol, expriceCol, discountCol, revenueCol, supcostCol, taxCol, commitdateCol, shipmodCol), true)


    val custuniqs = uniq("customer") _
    //val custKeyCol = LD2[String]("cust_key", new LazyMemCol(custuniqs(1)))
    //2 -> name
    //3 -> address
    val custCityCol = LD2[String]("cust_city", new LazyMemCol(custuniqs(4)))
    val custNationCol = LD2[String]("cust_nation", new LazyMemCol(custuniqs(5)))
    val custRegionCol = LD2[String]("cust_region", new LazyMemCol(custuniqs(6)))
    //7 -> Phone
    val custMarketCol = LD2[String]("cust_mkt_segment", new LazyMemCol(custuniqs(8)))
    val custLocation = BD2("Customer Location", Vector(custCityCol, custNationCol, custRegionCol), false)
    val custDims = BD2("Customer", Vector(custLocation, custMarketCol), true)


    val suppuniqs = uniq("supplier") _
    //val suppKeyCol = LD2[String]("supp_key", new LazyMemCol(suppuniqs(1)))
    //2 -> name
    //3 -> address
    val suppCityCol = LD2[String]("supp_city", new LazyMemCol(suppuniqs(4)))
    val suppNationCol = LD2[String]("supp_nation", new LazyMemCol(suppuniqs(5)))
    val suppRegionCol = LD2[String]("supp_region", new LazyMemCol(suppuniqs(6)))
    //6 -> Phone
    val suppDims= BD2("Supplier", Vector(suppCityCol, suppNationCol, suppRegionCol), false)
    //val suppDims = BD2("Supplier", Vector(suppLocation), true)


    //1 -> date in numbers
    //2 -> date in string
    //3 -> day in week (string)
    //4 -> month
    //5 -> year
    //6 -> year_monthnum
    //7 -> year_monthstr
    //8 -> day in week (number)
    //9 -> day in month
    //10 -> day in year
    //11 -> month in year
    //12 -> week in year
    //13 -> season
    //14 -> last day in week?
    //15 -> last day in month?
    //16 -> isHoliday?
    //17 -> isweekday?

    val partuniqs = uniq("part") _

     //val pidCol = LD2[String]("part_key", new LazyMemCol(partuniqs(1)))
    // 2 -> name
     val mfgrCol = LD2[String]("mfgr", new LazyMemCol(partuniqs(3)))
     val catCol = LD2[String]("category", new LazyMemCol(partuniqs(4)))
    val brandCol = LD2[String]("brand", new LazyMemCol(partuniqs(5)))
     val colorCol = LD2[String]("color", new LazyMemCol(partuniqs(6)))
    val typeCol = LD2[String]("type", new LazyMemCol(partuniqs(7)))
    val sizeCol = LD2[Int]("size", StaticNatCol.fromFile(partuniqs(8)))
    val containerCol = LD2[String]("container", new LazyMemCol(partuniqs(9)))

    val partDims = BD2("Part", Vector(mfgrCol, catCol, brandCol, colorCol, typeCol, sizeCol, containerCol), true)
    //val partDims = BD2("Part", Vector(pidCol, part1), false)

    val allDims = Vector(ordDims, custDims, suppDims, partDims)
    val sch = new StaticSchema2(allDims)

    //sch.columnVector.map(c => s"${c.name} has ${c.encoder.bits.size} bits = ${c.encoder.bits}").foreach(println)
    //println("Total = "+sch.n_bits)

    //sch.queriesUpto(25).mapValues(_.size).toList.sortBy(_._1).foreach{case (k,v) => println(s"$k\t $v")}
    sch
  }

  def readTbl(name: String, colIdx: Vector[Int]) = {
    Profiler.noprofile(s"readTbl$name") {
      val size = CSVReader.iterator(new FileReader(s"$folder/${name}.tbl"), '|').size
      val tbl = CSVReader.iterator(new FileReader(s"$folder/${name}.tbl"), '|')
      size -> tbl.map { r => colIdx.map(i => r(i))}
    }
  }
  def fetchPart(name: String, colIdx: Vector[Int])(i: Int) = {
    val num =  String.format("%03d",Int.box(i))
    val n2 = name + "." + num
    //println("Reading " + n2)
    readTbl(n2, colIdx)
  }

  def generate( ) = ???
  override def generate2() = {

    //val date = readTbl("date", Vector(0, 2)).map(d => d.head -> Vector(sdf.parse(d(0)), d(1))).toMap

    val custs = readTbl("customer", Vector(0, 3, 4, 5, 7))._2.map(d => d.head -> d.tail).toMap
    val parts = readTbl("part", Vector(0, 2, 3, 4, 5, 6, 7, 8))._2.map(d => d.head -> d.tail).toMap
    val supps = readTbl("supplier", Vector(0, 3, 4, 5))._2.map(d => d.head -> d.tail).toMap

    val sch = schemaInstance

    def joinFunc(r: IndexedSeq[String]) = {
      val oid = r(0)
      val ln = r(1)
      val cid = r(2)
      val pid = r(3)
      val sid = r(4)
      val orderdateid = r(5)
      val oprio = r(6)
      val sprio = r(7)
      val qty = r(8)
      val ext_price = r(9)
      val discount = r(11)
      val revenue = r(12)
      val supcost = r(13)
      val tax = r(14)
      val commitdate = r(15)
      val shipmod = r(16)

      val ordVales = Vector(orderdateid, oprio, sprio, qty, ext_price, discount, revenue, supcost, tax, commitdate, shipmod)
      val key = ordVales ++
        //date(dateid) ++
        custs(cid) ++
      supps(sid) ++
        parts(pid)
      assert(key.size == sch.columnVector.size)

      val value = ext_price.toLong * (100 - discount.toInt) * (100 + tax.toInt) / 10000
      sch.encode_tuple(key) -> value
    }

    implicit val ec = ExecutionContext.global
    val los = if(sf < 1)
      Vector(readTbl("lineorder", (0 until 17).toVector))
    else
      (0 until sf*10).map { i => fetchPart("lineorder", (0 until 17).toVector)(i) }

    //println("LO Parts  = " + los.size)
    val jos = los.map{ case (n, it) => n -> it.map(joinFunc)}

    jos
  }
}

object SSBGen {
  def main(args: Array[String])  {
    val cg = SSB(100)

    cg.saveBase()
    val maxD = 30 // >15+14, so never passes threshold
    List(
      (15, 14)
    ).map { case (logN, minD) =>
      cg.saveRMS(logN, minD, maxD)
      cg.saveSMS(logN, minD, maxD)
    }

  }
}

