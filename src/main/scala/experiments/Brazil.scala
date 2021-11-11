package experiments

import backend.CBackend
import breeze.io.CSVReader
import core.{DataCube, RandomizedMaterializationScheme}
import frontend.generators.Iowa.read
import frontend.schema.{LD2, StructuredDynamicSchema}
import frontend.schema.encoders.{DateCol, MemCol}
import core.RationalTools._
import java.io.FileReader
import java.util.Date

object Brazil {

  def readCSV(name: String, cols: Vector[String]) = {
    val folder = "/Users/sachin/Downloads/brazil"
    val csv = CSVReader.read(new FileReader(s"$folder/olist_${name}_dataset.csv"))
    val header = csv.head
    val colIdx = cols.map(c => header.indexOf(c))
    csv.tail.map{r => colIdx.map(i => r(i))}
  }

  def read() = {
    val custs = readCSV("customers", Vector("customer_id", "customer_zip_code_prefix", "customer_city", "customer_state")).map(d => d(0) -> d.tail).toMap//.withDefaultValue(Vector("Cust Zip", "Cust City", "Cust State"))
    val orderitems = readCSV("order_items", Vector("order_id", "order_item_id", "product_id", "seller_id", "price"))
    val orders = readCSV("orders", Vector("order_id", "customer_id", "order_purchase_timestamp") ).map(d => d(0) -> d.tail).toMap//.withDefaultValue(Vector("Cust Id", "Time"))
    val products = readCSV("products", Vector("product_id", "product_category_name")).map(d => d(0) -> Vector(d(1))).toMap//.withDefaultValue(Vector("Prod Category"))
    val sellers = readCSV("sellers", Vector("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state")).map(d => d(0) -> d.tail).toMap//.withDefaultValue(Vector("Sell Zip", "Sell City", "Sell State"))

    val join = orderitems.map {  r =>
      val oid = r(0)
      val iid = r(1)
      val pid = r(2)
      val sid = r(3)
      val price = r(4)
      val order = orders(oid)
      val cid = order(0)
      val cust = custs(cid)
      val prod = products(pid)
      val sel = sellers(sid)
      Vector(oid, iid, order(1), order(0), cust(0), cust(1), cust(2), pid, prod(0), sid, sel(0), sel(1), sel(2)) -> (price.toDouble * 100).toLong
    }
    /*
       "order_id", "order_item_id"
        "order_purchase_timestamp"
        "customer_id",  "customer_zip_code_prefix", "customer_city", "customer_state"
        "product_id", "product_category_name"
        "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"

     */

    val order_id = new LD2[String]("order_id", new MemCol)
    val o_item_id = new LD2[String]("order_item_id", new MemCol)
    val orderDim = Vector(order_id, o_item_id)

    val time = new LD2[Date]("order_purchase_timestamp", new DateCol(2016))
    val timeDim = Vector(time)

    val cust_id = new LD2[String]("customer_id", new MemCol)
    val cust_zip = new LD2[String]("customer_zip_code_prefix", new MemCol)
    val cust_city = new LD2[String]("customer_city", new MemCol)
    val cust_state = new LD2[String]("customer_state", new MemCol)
    val custDim = Vector(cust_id, cust_zip, cust_city, cust_state)

    val prod_id = new LD2[String]("product_id", new MemCol)
    val prod_cat = new LD2[String]("product_category_name", new MemCol)
    val prodDim = Vector(prod_id, prod_cat)

    val sel_id = new LD2[String]("seller_id", new MemCol)
    val sel_zip = new LD2[String]("seller_zip_code_prefix", new MemCol)
    val sel_city = new LD2[String]("seller_city", new MemCol)
    val sel_state = new LD2[String]("seller_state", new MemCol)
    val selDim = Vector(sel_id, sel_zip, sel_city, sel_state)

    val sch = new StructuredDynamicSchema(orderDim ++ timeDim ++ custDim ++ prodDim ++ selDim)
    val r = join.map { case (k, v) => sch.encode_tuple(k) -> v}
    sch.columnVector.foreach(_.encoder.refreshBits)
    (sch, r)
  }

  def save( lrf: Double, lbase: Double) = {
    val (sch, r) = read()
    val rf = math.pow(10, lrf)
    val base = math.pow(10, lbase)
    val dc = new DataCube(RandomizedMaterializationScheme(sch.n_bits, rf, base))
    val name = "brazil"
    sch.save(name)
    dc.build(CBackend.b.mk(sch.n_bits, r.toIterator))
    dc.save2(s"${name}_${lrf}_${lbase}")
  }

  def load(lrf: Double, lbase: Double) = {
    val inputname = "brazil"
    val sch = StructuredDynamicSchema.load(inputname)
    sch.columnVector.map(c => c.name -> c.encoder.bits).foreach(println)
    val dc = DataCube.load2(s"${inputname}_${lrf}_${lbase}")
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

    dateQ.flatMap{ q1 => custQ.flatMap{ q2 => prodQ.flatMap{q3 => selQ.map{q4 => q1 ++ q2 ++ q3 ++ q4}}}}.toList.sortBy(_.length)
  }
  def main(args: Array[String]) = {
    //save(-25.5, 0.19)
    val (sch, dc) = load(-25.5, 0.19)
    val qs = queries(sch).filter(x => x.length >= 4 && x.length <= 10)
    val expt = new UniformSolverExpt(dc, "brazil2")
    expt.compare(List(153, 132, 115, 104, 90, 77, 68, 57, 46, 33))
    //qs.foreach(q => expt.compare(q))
  }

}
