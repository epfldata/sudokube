package frontend.generators

import backend.CBackend
import breeze.io.CSVReader
import core.{DataCube, RandomizedMaterializationScheme}
import experiments.UniformSolverExpt
import frontend.schema.encoders.{DateCol, MemCol, NatCol}
import frontend.schema.{BD2, LD2, StructuredDynamicSchema}
import util.Profiler
import core.RationalTools._

import java.io.FileReader
import java.util.Date

object SSB {
  var sf = 1

  def readTbl(name: String, colIdx: Vector[Int]) = {
    Profiler(s"readTbl$name") {
      val folder = s"tabledata/SSB/sf${sf}"
      val tbl = CSVReader.read(new FileReader(s"$folder/${name}.tbl"), '|')
      tbl.map { r => colIdx.map(i => r(i)) }
    }
  }

  def read() = {
    val date = readTbl("date", Vector(0, 2)).map(d => d.head -> d).toMap
    val custs = readTbl("customer", Vector(0, 3, 4, 5, 7)).map(d => d.head -> d).toMap
    val parts = readTbl("part", Vector(0, 2, 3, 4, 5, 6, 7, 8)).map(d => d.head -> d).toMap
    val supps = readTbl("supplier", Vector(0, 3, 4, 5)).map(d => d.head -> d).toMap
    val lineorder = readTbl("lineorder", Vector(0, 1, 2, 3, 4, 5, 6, 7, 9, 16))

    val join = Profiler("JOIN") {
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
    /* 00 */ val oidCol = LD2[String]("order_key", new MemCol)
    /* 01 */ val lnCol = LD2[Int]("line_number", new NatCol)
    /* 02 */ val oprioCol = LD2[String]("ord_priority", new MemCol)
    /* 03 */ val shiprioCol = LD2[String]("ship_priority", new MemCol)
    /* 04 */ val shipmodCol = LD2[String]("ship_mode", new MemCol)

    val ordDims = BD2("Order", Vector(oidCol, lnCol, oprioCol, shiprioCol, shipmodCol), false)

    /* 05 */ val dateCol = LD2[Date]("date", new DateCol(1992))
    /* 06 */ val weekdayCol = LD2[String]("day_of_week", new MemCol)

    val dateDims = BD2("Time",Vector(dateCol, weekdayCol), false)

    /* 07 */ val cidCol = LD2[String]("cust_key", new MemCol)
    /* 08*/ val ccityCol = LD2[String]("cust_city", new MemCol)
    /* 09 */ val cnatCol = LD2[String]("cust_nation", new MemCol)
    /* 10 */ val cregCol = LD2[String]("cust_region", new MemCol)
    /* 11 */ val cmkCol = LD2[String]("cust_mkt_segment", new MemCol)

    val custDims = BD2("Customer",Vector(cidCol, ccityCol, cnatCol, cregCol, cmkCol), false)

    /* 12 */ val pidCol = LD2[String]("part_key", new MemCol)
    /* 13 */ val mfgrCol = LD2[String]("mfgr", new MemCol)
    /* 14 */ val catCol = LD2[String]("category", new MemCol)
    /* 15 */ val brandCol = LD2[String]("brand", new MemCol)
    /* 16 */ val colorCol = LD2[String]("color", new MemCol)
    /* 17 */ val typeCol = LD2[String]("type", new MemCol)
    /* 18 */ val sizeCol = LD2[Int]("size", new NatCol)
    /* 19 */ val containerCol = LD2[String]("container", new MemCol)

    val part1 = BD2("Part Properties", Vector(mfgrCol, catCol, brandCol, colorCol, typeCol, sizeCol, containerCol), true)
    val partDims = BD2("Part", Vector(pidCol, part1), false)

    /* 20 */ val sidCol = LD2[String]("sup_key", new MemCol)
    /* 21 */ val scityCol = LD2[String]("sup_city", new MemCol)
    /* 22 */ val snatCol = LD2[String]("sup_nation", new MemCol)
    /* 23 */ val sregCol = LD2[String]("sup_region", new MemCol)

    val supDims = BD2("Supplier", Vector(sidCol, scityCol, snatCol, sregCol), false)
    val allDims = Vector(ordDims, dateDims, custDims, partDims, supDims)
    val sch = new StructuredDynamicSchema(allDims)

    //join.take(10).map(r => r._1.zip(allDims.map(_.name)).mkString("   ")).foreach(println)

    val r = join.zipWithIndex.map { case ((k, v), i) =>
      if (i % 100000 == 0) {
        println(s"Encoding $i/${join.length}")
        Profiler.print()
      }
      sch.encode_tuple(k) -> v
    }

    sch.columnVector.foreach(_.encoder.refreshBits)
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
    //save(-27, 0.19)
    val (sch, dc) = load(-27, 0.19)
    val qs = sch.queries.filter(x => x.length >= 4 && x.length <= 10)
    val expt = new UniformSolverExpt(dc, s"SSB-sf${sf}")
    qs.foreach(q => expt.compare(q))
  }


}
