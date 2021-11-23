package frontend.generators

import backend.CBackend
import breeze.io.CSVReader
import core.{DataCube, RandomizedMaterializationScheme, Rational}
import experiments.UniformSolverExpt
import frontend.schema.encoders.{DateCol, MemCol, NatCol}
import frontend.schema.{BD2, BitPosRegistry, LD2, StructuredDynamicSchema}
import util.Profiler
import core.RationalTools._

import java.io.FileReader
import java.text.SimpleDateFormat
import java.util.Date

object SSB {
  var sf = 1

  def readTbl(name: String, colIdx: Vector[Int]) = {
    Profiler.noprofile(s"readTbl$name") {
      val folder = s"tabledata/SSB/sf${sf}"
      val tbl = CSVReader.read(new FileReader(s"$folder/${name}.tbl"), '|')
      tbl.map { r => colIdx.map(i => r(i)) }
    }
  }

  def read() = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date = readTbl("date", Vector(0, 2)).map(d => d.head -> Vector(sdf.parse(d(0)), d(1))).toMap
    val custSeq = readTbl("customer", Vector(0, 3, 4, 5, 7))
    val custs = custSeq.map(d => d.head -> d).toMap
    val partSeq = readTbl("part", Vector(0, 2, 3, 4, 5, 6, 7, 8))
    val parts = partSeq.map(d => d.head -> d).toMap
    val suppSeq = readTbl("supplier", Vector(0, 3, 4, 5))
    val supps = suppSeq.map(d => d.head -> d).toMap
    val lineorder = readTbl("lineorder", Vector(0, 1, 2, 3, 4, 5, 6, 7, 9, 16))

    val join = Profiler.noprofile("JOIN") {
      lineorder.map { r =>
        val oid = r(0)
        val ln = r(1)
        val cid = r(2)
        val pid = r(3)
        val sid = r(4)
        val dateid = r(5)
        val oprio = r(6)
        val sprio = r(7)
        val price = r(8)
        val shipmod = r(9)

        (Vector(oid, ln, oprio, sprio, shipmod) ++ date(dateid) ++ custs(cid) ++ parts(pid) ++ supps(sid)) -> (price.toDouble * 100).toLong

      }
    }
    implicit val reg = new BitPosRegistry
    val oidvals = lineorder.map(r => r(0)).distinct
    /* 00 */ val oidCol = LD2[String]("order_key", new MemCol(oidvals))
    val lidmax = lineorder.map(r => r(1)).map(_.toInt).max
    /* 01 */ val lnCol = LD2[Int]("line_number", new NatCol(lidmax))
    val opriovals = lineorder.map(r => r(6)).distinct
    /* 02 */ val oprioCol = LD2[String]("ord_priority", new MemCol(opriovals))
    val shipriovals = lineorder.map(r => r(7)).distinct
    /* 03 */ val shiprioCol = LD2[String]("ship_priority", new MemCol(shipriovals))
    val shipmodvals = lineorder.map(r => r(9)).distinct
    /* 04 */ val shipmodCol = LD2[String]("ship_mode", new MemCol(shipmodvals))

    val ordDims = BD2("Order", Vector(oidCol, lnCol, oprioCol, shiprioCol, shipmodCol), false)


    /* 05 */ val dateCol = LD2[Date]("date", new DateCol(1992, 1998, true, true))
    val weekdaysvals = List("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
    /* 06 */ val weekdayCol = LD2[String]("day_of_week", new MemCol(weekdaysvals))

    val dateDims = BD2("Time",Vector(dateCol, weekdayCol), false)

    val cidvals = custSeq.map(r => r(0)).distinct
    /* 07 */ val cidCol = LD2[String]("cust_key", new MemCol(cidvals))
    val cityvals = (custSeq.map(r => r(1)) ++ suppSeq.map(r => r(1))).distinct
    /* 08*/ val ccityCol = LD2[String]("cust_city", new MemCol(cityvals))
    val nationvals = (custSeq.map(r => r(2)) ++ suppSeq.map(r => r(2))).distinct
    /* 09 */ val cnatCol = LD2[String]("cust_nation", new MemCol(nationvals))
    val regionvals = (custSeq.map(r => r(3)) ++ suppSeq.map(r => r(3))).distinct
    /* 10 */ val cregCol = LD2[String]("cust_region", new MemCol(regionvals))
    val mktvals = custSeq.map(r => r(4)).distinct
    /* 11 */ val cmkCol = LD2[String]("cust_mkt_segment", new MemCol(mktvals))

    val custDims = BD2("Customer",Vector(cidCol, ccityCol, cnatCol, cregCol, cmkCol), false)

    val pidvals = partSeq.map( r => r(0)).distinct
    /* 12 */ val pidCol = LD2[String]("part_key", new MemCol(pidvals))
    val mfgrvals = partSeq.map(r => r(1)).distinct
    /* 13 */ val mfgrCol = LD2[String]("mfgr", new MemCol(mfgrvals))
    val catvals = partSeq.map(r => r(2)).distinct
    /* 14 */ val catCol = LD2[String]("category", new MemCol(catvals))
    val brandvals = partSeq.map(r => r(3)).distinct
    /* 15 */ val brandCol = LD2[String]("brand", new MemCol(brandvals))
    val colorvals = partSeq.map(r => r(4)).distinct
    /* 16 */ val colorCol = LD2[String]("color", new MemCol(colorvals))
    val typevals = partSeq.map(r => r(5)).distinct
    /* 17 */ val typeCol = LD2[String]("type", new MemCol(typevals))
    val sizemax = partSeq.map(r => r(6).toInt).max
    /* 18 */ val sizeCol = LD2[Int]("size", new NatCol(sizemax))
    val contvals = partSeq.map(r => r(7)).distinct
    /* 19 */ val containerCol = LD2[String]("container", new MemCol(contvals))

    val part1 = BD2("Part Properties", Vector(mfgrCol, catCol, brandCol, colorCol, typeCol, sizeCol, containerCol), true)
    val partDims = BD2("Part", Vector(pidCol, part1), false)

    val supidvals = suppSeq.map(r => r(0)).distinct
    /* 20 */ val sidCol = LD2[String]("sup_key", new MemCol(supidvals))
    /* 21 */ val scityCol = LD2[String]("sup_city", new MemCol(cityvals))
    /* 22 */ val snatCol = LD2[String]("sup_nation", new MemCol(nationvals))
    /* 23 */ val sregCol = LD2[String]("sup_region", new MemCol(regionvals))

    val supDims = BD2("Supplier", Vector(sidCol, scityCol, snatCol, sregCol), false)
    val allDims = Vector(ordDims, dateDims, custDims, partDims, supDims)
    val sch = new StructuredDynamicSchema(allDims)

    //join.take(10).map(r => r._1.zip(allDims.map(_.name)).mkString("   ")).foreach(println)

    val r = join.zipWithIndex.map { case ((k, v), i) =>
      if (i % 500000 == 0) {
        println(s"Encoding $i/${join.length}")
        Profiler.print()
        //sch.columnVector.map(c => (c.name, c.encoder.isRange, c.encoder.bits)).filter(x => !x._2 || (!x._3.isEmpty && (x._3.head != x._3.last + x._3.length-1))).foreach(println)
      }
      sch.encode_tuple(k) -> v
    }

    (sch, r)
  }


  def save(lrf: Double, lbase: Double) = {
    val (sch, r) = read()
    val rf = math.pow(10, lrf)
    val base = math.pow(10, lbase)
    val dc = new DataCube(RandomizedMaterializationScheme(sch.n_bits, rf, base))
    val name = s"SSB-sf${sf}"
    sch.save(name)
    dc.build(CBackend.b.mk(sch.n_bits, r.toIterator))
    dc.save2(s"${name}_${lrf}_${lbase}")
    (sch, dc)
  }

  def loadAndSave(lrf1: Double, lbase1: Double, lrf2: Double, lbase2: Double) = {

    val rf1 = math.pow(10, lrf1)
    val base1 = math.pow(10, lbase1)
    val rf2 = math.pow(10, lrf1)
    val base2 = math.pow(10, lbase1)
    val name = s"SSB-sf${sf}"
    val dc1 = DataCube.load2(s"${name}_${lrf1}_${lbase1}")
    val dc2 = new DataCube(RandomizedMaterializationScheme(dc1.m.n_bits, rf2, base2))

    dc2.buildFrom(dc1)
    dc2.save2(s"${name}_${lrf2}_${lbase2}")
  }

  def load(lrf: Double, lbase: Double) = {
    val inputname = s"SSB-sf${sf}"
    val sch = StructuredDynamicSchema.load(inputname)
    sch.columnVector.map(c => c.name -> c.encoder.bits).foreach(println)
    val dc = DataCube.load2(s"${inputname}_${lrf}_${lbase}")
    (sch, dc)
  }


  def main(args: Array[String]) = {
    //println(Runtime.getRuntime.maxMemory()/(1 << 30).toDouble)
    //read()
    var lrf = 0.0
    var lbase = -1.0
    if(args.length > 0) {
      sf = args(0).toInt
      lrf = args(1).toDouble
      lbase = args(2).toDouble
    }
    println(s"SF = $sf")
    val (sch, dc) = save(lrf, lbase)

    //val (sch, dc) = load(lrf, lbase)
    //val qs = sch.queries.filter(x => x.length >= 4 && x.length <= 10)
    //val expt = new UniformSolverExpt[Double](dc, s"SSB-sf${sf}")
    //expt.compare(List(40, 14, 4, 3, 2, 109, 86, 52, 23, 22))
    //qs.foreach(q => expt.compare(q))

    Profiler.print()
  }


}
