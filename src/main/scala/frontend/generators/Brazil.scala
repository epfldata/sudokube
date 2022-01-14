package frontend.generators

import backend.CBackend
import breeze.io.CSVReader
import core.{DataCube, RandomizedMaterializationScheme}
import experiments.UniformSolverFullExpt
import frontend.schema.encoders.{DateCol, MemCol}
import frontend.schema.{BD2, BitPosRegistry, LD2, StructuredDynamicSchema}
import core.RationalTools._

import java.io.FileReader
import java.util.Date

object Brazil extends CubeGenerator("Brazil"){

  def readCSV(name: String, cols: Vector[String]) = {
    val folder = "tabledata/Brazil"
    val csv = CSVReader.read(new FileReader(s"$folder/olist_${name}_dataset.csv"))
    val header = csv.head
    val colIdx = cols.map(c => header.indexOf(c))
    csv.tail.map { r => colIdx.map(i => r(i)) }
  }

  def generate() = {
    val custs = readCSV("customers", Vector("customer_id", "customer_zip_code_prefix", "customer_city", "customer_state")).map(d => d(0) -> d.tail).toMap //.withDefaultValue(Vector("Cust Zip", "Cust City", "Cust State"))
    val orderitems = readCSV("order_items", Vector("order_id", "order_item_id", "product_id", "seller_id", "price"))
    val orders = readCSV("orders", Vector("order_id", "customer_id", "order_purchase_timestamp")).map(d => d(0) -> d.tail).toMap //.withDefaultValue(Vector("Cust Id", "Time"))
    val products = readCSV("products", Vector("product_id", "product_category_name")).map(d => d(0) -> Vector(d(1))).toMap //.withDefaultValue(Vector("Prod Category"))
    val sellers = readCSV("sellers", Vector("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state")).map(d => d(0) -> d.tail).toMap //.withDefaultValue(Vector("Sell Zip", "Sell City", "Sell State"))

    val join = orderitems.map { r =>
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

    implicit val bitR = new BitPosRegistry
    val order_id =  LD2[String]("order_id", new MemCol)
    val o_item_id =  LD2[String]("order_item_id", new MemCol)
    val time =  LD2[Date]("order_purchase_timestamp", new DateCol(2016, 2018, true, true, true, true, true))
    val orderDim =  BD2("Order", Vector(order_id, o_item_id, time), false)

    val cust_id =  LD2[String]("customer_id", new MemCol)
    val cust_zip =  LD2[String]("customer_zip_code_prefix", new MemCol)
    val cust_city =  LD2[String]("customer_city", new MemCol)
    val cust_state =  LD2[String]("customer_state", new MemCol)
    val custDim =  BD2("Customer", Vector(cust_id, cust_zip, cust_city, cust_state), false)

    val prod_id =  LD2[String]("product_id", new MemCol)
    val prod_cat =  LD2[String]("product_category_name", new MemCol)
    val prodDim =  BD2("Product", Vector(prod_id, prod_cat), false)

    val sel_id =  LD2[String]("seller_id", new MemCol)
    val sel_zip =  LD2[String]("seller_zip_code_prefix", new MemCol)
    val sel_city =  LD2[String]("seller_city", new MemCol)
    val sel_state =  LD2[String]("seller_state", new MemCol)
    val selDim =  BD2("Seller", Vector(sel_id, sel_zip, sel_city, sel_state), false)

    val sch = new StructuredDynamicSchema(Vector(orderDim, custDim, prodDim, selDim))
    val r = join.map { case (k, v) => sch.encode_tuple(k) -> v }
    (sch, r)
  }
}
