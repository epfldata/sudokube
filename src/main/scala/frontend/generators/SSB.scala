package frontend.generators

import backend.CBackend
import breeze.io.CSVReader
import core.{DataCube, RandomizedMaterializationScheme, Rational}
import experiments.UniformSolverExpt
import frontend.schema.encoders.{DateCol, MemCol, NatCol}
import frontend.schema.{BD2, BitPosRegistry, LD2, StructuredDynamicSchema}
import util.{Profiler, Util}
import core.RationalTools._

import java.io.FileReader
import java.text.SimpleDateFormat
import java.time.Duration
import scala.concurrent.duration.{Duration => ScalaDur}
import java.util.Date
import java.util.concurrent.{Callable, ExecutorService, Executors, FutureTask, ThreadPoolExecutor}
import scala.concurrent.{Await, ExecutionContext, Future}

case class SSB(sf: Int) extends CubeGenerator(s"SSB-sf$sf") {

  def readTbl(name: String, colIdx: Vector[Int]) = {
    Profiler.noprofile(s"readTbl$name") {
      val folder = s"tabledata/SSB/sf${sf}"
      val tbl = CSVReader.iterator(new FileReader(s"$folder/${name}.tbl"), '|')
      tbl.map { r => colIdx.map(i => r(i)) }.toList
    }
  }
  def fetchPart(name: String, colIdx: Vector[Int])(i: Int)(implicit ec: ExecutionContext) = {
    val num =  String.format("%03d",Int.box(i))
    val n2 = name + "." + num
    //println("Reading " + n2)
    Future{
      val r = readTbl(n2, colIdx)
      if(i % 100 == 0) print(s" R${i/10} ")
      r
    }

  }

  def generate() = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date = readTbl("date", Vector(0, 2)).map(d => d.head -> Vector(sdf.parse(d(0)), d(1))).toMap
    val custSeq = readTbl("customer", Vector(0, 3, 4, 5, 7))
    val custs = custSeq.map(d => d.head -> d).toMap
    val partSeq = readTbl("part", Vector(0, 2, 3, 4, 5, 6, 7, 8))
    val parts = partSeq.map(d => d.head -> d).toMap
    val suppSeq = readTbl("supplier", Vector(0, 3, 4, 5))
    val supps = suppSeq.map(d => d.head -> d).toMap

    def joinFunc(r: IndexedSeq[String]) = {
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

    implicit val ec = ExecutionContext.global

    val los = if(sf < 1)
      Vector(Future{readTbl("lineorder", Vector(0, 1, 2, 3, 4, 5, 6, 7, 9, 16))})
    else
      (0 until sf*10).map { i => fetchPart("lineorder.cut", (0 to 9).toVector)(i) }

    println("LO Parts  = " + los.size)
    val jos = los.indices.map(i => los(i).map(ls => {
      val res = ls.map(joinFunc)
      if (los.size < 100 || i % (los.size/100) == 0) print(s" J${(i*100)/los.size} ")
      res
    }))

    //val loss = Future.sequence(los).map(_.flatMap(_._2))
    //val joss = Future.sequence(jos).map(_.flatten)
    //val mloss = Await.result(loss, ScalaDur.Inf)
    //println("Merging lineorders complete")
    //val mjoss = Await.result(joss, ScalaDur.Inf)
    //println("Merging join complete")
    //(mloss, mjoss)

    def getDistinct[T](id: Int) = {
      val futdists = los.map { fvs =>
        fvs.map { vs =>
          vs.map{r =>
            r(id).asInstanceOf[T]}.distinct
        }
      }
      val f  = Future.sequence(futdists).map(_.flatten.distinct)
      val res = Await.result(f, ScalaDur.Inf)
      println("Distinct "+id)
      res
    }

    def getMax[T:Numeric](id: Int) = {
      val futdists = los.map { fvs =>
        fvs.map { vs =>
          vs.map{r =>
            implicitly[Numeric[T]].fromInt(r(id).toInt)}.max
        }
      }
      val f  = Future.sequence(futdists).map(_.max)
      val res = Await.result(f, ScalaDur.Inf)
      println("Max "+id)
      res
    }


    implicit val reg = new BitPosRegistry
    val oidvals = getDistinct(0)
    /* 00 */ val oidCol = LD2[String]("order_key", new MemCol(oidvals))
    val lidmax = getMax[Int](1)
    /* 01 */ val lnCol = LD2[Int]("line_number", new NatCol(lidmax))
    val opriovals = getDistinct(6)
    /* 02 */ val oprioCol = LD2[String]("ord_priority", new MemCol(opriovals))
    val shipriovals = getDistinct(7)
    /* 03 */ val shiprioCol = LD2[String]("ship_priority", new MemCol(shipriovals))
    val shipmodvals = getDistinct(9)
    /* 04 */ val shipmodCol = LD2[String]("ship_mode", new MemCol(shipmodvals))

    val ordDims = BD2("Order", Vector(oidCol, lnCol, oprioCol, shiprioCol, shipmodCol), false)


    /* 05 */ val dateCol = LD2[Date]("date", new DateCol(1992, 1998, true, true))
    val weekdaysvals = List("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
    /* 06 */ val weekdayCol = LD2[String]("day_of_week", new MemCol(weekdaysvals))

    val dateDims = BD2("Time", Vector(dateCol, weekdayCol), false)

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

    val custDims = BD2("Customer", Vector(cidCol, ccityCol, cnatCol, cregCol, cmkCol), false)

    val pidvals = partSeq.map(r => r(0)).distinct
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

    val resfs = jos.map { fjo =>
      fjo.map { jo => jo.map { case (k, v) => sch.encode_tuple(k) -> v } }
    }
    val resf = Future.sequence(resfs).map(_.flatten)
    val r  = Await.result(resf, ScalaDur.Inf)

    (sch, r)
  }
}

object SSBTest {
  def main(args: Array[String])  {
    val cg = SSB(100)
    //cg.saveBase()
    cg.buildFromBase(-32, 0.19)
  }
}

