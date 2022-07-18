import backend.CBackend
import core.DataCube
import core.materialization.RandomizedMaterializationScheme
import frontend.schema.DynamicSchema
import org.scalatest.{FreeSpec, Matchers}
import util.Bits

class SchemaSpec extends FreeSpec with Matchers {

  val investmentCSV = List(
    Array("Google", "1998", "10"),
    Array("Greenplum", "2005", "200"),
    Array("Greenplum", "2007", "1500"),
    Array("Greenplum", "2008", "20000"),
    Array("Snowflake", "2011", "150"),
    Array("Snowflake", "2013", "3500"),
    Array("Snowflake", "2015", "30000"),
  )
  "DynamicSchema" - {
    val sch = new DynamicSchema()
    val R = sch.read("example-data/investments.json", Some("k_amount"), _.asInstanceOf[Int].toLong)

    "must read tuples correctly" in {
      val d1 = R.map { case (k, v) => sch.decode_tuple(k).map({
        case (_, None) => "NULL"
        case (_, Some(u)) => u.toString
        case (_, u) => u.toString
      }).toList -> v
      }
      val d2 = investmentCSV.map(a => List(a(1), a(0)) -> a(2).toLong)
      assert(d1 sameElements d2)
    }

    "must have correct bits " in {
      val l1 = sch.columnList.map(c => c._1 -> c._2.bits.toList)
      val l2 = List("date" -> List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), "company" -> List(0, 12))
      assert(l1.sameElements(l2))
    }

    val basecuboid = CBackend.b.mk(sch.n_bits, R.toIterator)

    val matscheme = new RandomizedMaterializationScheme(sch.n_bits, 8, 4)
    val dc = new DataCube(matscheme)
    dc.build(basecuboid)

    def perm_func(query: IndexedSeq[Int]) = {
      val query_sorted = query.sorted
      val perm = query.map(b => query_sorted.indexOf(b)).toArray
      Bits.permute_bits(query.size, perm)
    }

    def permuted_result(query: IndexedSeq[Int]) = {
      val permf = perm_func(query)
      val result0 = dc.naive_eval(query.sorted)
      val result1 = new Array[Int](result0.size)
      result0.indices.foreach{i => result1(permf(i)) = result0(i).toInt}
      result1
    }

    "must have correct result for query1 " in {
      val query = sch.columns("company").bits ++ sch.columns("date").bits.take(1) //odd even years for every company
      val result = permuted_result(query)
      val result_orig = Array(0, 10, 20000, 0, 0, 0, 1700, 33650)
      assert(result_orig sameElements result)
    }

    "must have correct result for query2 " in {
      val query = sch.columns("company").bits ++ sch.columns("date").bits.takeRight(1) //top and botton 1024 years for every company
      val result = permuted_result(query)
      val result_orig = Array(0, 0, 0, 0, 0, 10, 21700, 33650)
      assert(result_orig sameElements result)
    }

    "must have correct result for query3 " in {
      val query = sch.columns("company").bits // for every company
      val result = permuted_result(query)
      val result_orig = Array(0, 10, 21700, 33650)
      assert(result_orig sameElements result)
    }
  }
}
