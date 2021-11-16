package experiments

import backend.CBackend
import breeze.io.CSVReader
import core.{DataCube, RandomizedMaterializationScheme}
import frontend.schema.{LD2, StructuredDynamicSchema}
import frontend.schema.encoders.{DateCol, MemCol, NatCol}
import core.RationalTools._
import util.Profiler

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
    /* 00 */ val oidCol = new LD2[String]("order_key", new MemCol)
    /* 01 */ val lnCol = new LD2[Int]("line_number", new NatCol)
    /* 02 */ val oprioCol = new LD2[String]("ord_priority", new MemCol)
    /* 03 */ val shiprioCol = new LD2[String]("ship_priority", new MemCol)
    /* 04 */ val shipmodCol = new LD2[String]("ship_mode", new MemCol)

    val ordDims = Vector(oidCol, lnCol, oprioCol, shiprioCol, shipmodCol)

    /* 05 */ val dateCol = new LD2[Date]("date", new DateCol(1992))
    /* 06 */ val weekdayCol = new LD2[String]("day_of_week", new MemCol)

    val dateDims = Vector(dateCol, weekdayCol)

    /* 07 */ val cidCol = new LD2[String]("cust_key", new MemCol)
    /* 08*/ val ccityCol = new LD2[String]("cust_city", new MemCol)
    /* 09 */ val cnatCol = new LD2[String]("cust_nation", new MemCol)
    /* 10 */ val cregCol = new LD2[String]("cust_region", new MemCol)
    /* 11 */ val cmkCol = new LD2[String]("cust_mkt_segment", new MemCol)

    val custDims = Vector(cidCol, ccityCol, cnatCol, cregCol, cmkCol)

    /* 12 */ val pidCol = new LD2[String]("part_key", new MemCol)
    /* 13 */ val mfgrCol = new LD2[String]("mfgr", new MemCol)
    /* 14 */ val catCol = new LD2[String]("category", new MemCol)
    /* 15 */ val brandCol = new LD2[String]("brand", new MemCol)
    /* 16 */ val colorCol = new LD2[String]("color", new MemCol)
    /* 17 */ val typeCol = new LD2[String]("type", new MemCol)
    /* 18 */ val sizeCol = new LD2[Int]("size", new NatCol)
    /* 19 */ val containerCol = new LD2[String]("container", new MemCol)

    val partDims = Vector(pidCol, mfgrCol, catCol, brandCol, colorCol, typeCol, sizeCol, containerCol)

    /* 20 */ val sidCol = new LD2[String]("sup_key", new MemCol)
    /* 21 */ val scityCol = new LD2[String]("sup_city", new MemCol)
    /* 22 */ val snatCol = new LD2[String]("sup_nation", new MemCol)
    /* 23 */ val sregCol = new LD2[String]("sup_region", new MemCol)

    val supDims = Vector(sidCol, scityCol, snatCol, sregCol)
    val allDims = ordDims ++ dateDims ++ custDims ++ partDims ++ supDims
    val sch = new StructuredDynamicSchema(allDims)

    //join.take(10).map(r => r._1.zip(allDims.map(_.name)).mkString("   ")).foreach(println)

    val r = join.zipWithIndex.map { case ((k, v), i) =>
      if(i % 100000 == 0) {
        println(s"Encoding $i/${join.length}")
        Profiler.print()
      }
      sch.encode_tuple(k) -> v }

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

  def queries(sch: StructuredDynamicSchema) = {
    val ord = (2 to 4).map(i => sch.columnVector(i))
    val date = (5 to 6).map(i => sch.columnVector(i))
    val cust = (7 to 11).map(i => sch.columnVector(i))
    val prod = (12 to 19).map(i => sch.columnVector(i))
    val sel = (20 to 23).map(i => sch.columnVector(i))

    val ordQ = ord.map(_.encoder.queries).reduce(_ union _)
    val dateQ = date.map(_.encoder.queries).reduce(_ union _)
    val custQ = cust.map(_.encoder.queries).reduce(_ union _)
    val selQ = sel.map(_.encoder.queries).reduce(_ union _)


    val prodQ = prod.map(_.encoder.queries).foldLeft(Set[List[Int]]()){case (acc, cur) =>
    acc.flatMap{ qa => cur.map(qc => qa ++ qc)}
    }

    println("Order queries = " + ordQ.size)
    println("Date queries = " + dateQ.size)
    println("Cust queries = " + custQ.size)
    println("Product queries = " + prodQ.size)
    println("Seller queries = " + selQ.size)

    List(ordQ, dateQ, custQ, prodQ, selQ).foldLeft(Set[List[Int]]()){case (acc, cur) =>
      acc.flatMap{ qa => cur.map(qc => qa ++ qc)}}.toList.sortBy(_.length)
  }

  def main(args: Array[String]) = {
    //println(Runtime.getRuntime.maxMemory()/(1 << 30).toDouble)
    //save(-27, 0.19)
    val (sch, dc) = load(-27, 0.19)
    val qs = queries(sch).filter(x => x.length >= 4 && x.length <= 10)
    val expt = new UniformSolverExpt(dc, s"SSB-sf${sf}")
    qs.foreach(q => expt.compare(q))
  }


}
