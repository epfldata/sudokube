//package ch.epfl.data.sudokube
package frontend
import core._
import core.solver.Rational
import core.solver.RationalTools._
import schema._


object Exploration {
  def col_names(sch: Schema) : IndexedSeq[String] = sch.columnList.map(_._1)

  /** distribution of values for column coln. */
  def dist(sch: Schema, dc: DataCube, coln: String
  ) : Seq[(String, Rational)] = {

    val q = sch.columnList.toMap.apply(coln).bits

/*
    val s = dc.solver[Rational](q, dc.m.n_bits)
    s.compute_bounds
    val result: List[Rational] = s.bounds.toList.map(_.lb.get)
*/
    val result = dc.naive_eval(q).toList.map(x => Rational(x.toInt, 1))

    val R = sch.decode_dim(q).zip(result)

    // pretty-printing
    R.map {
      case (str, cnt) => {
        val x = str(0).split("=")
        if(x.length == 2) Some((x(1), cnt))
        else None // a text comment may contain =
      }
    }.flatten
  }

  def nat_decode_dim(sch: DynamicSchema, coln: String, drop_bits: Int) : Seq[Range] = {
    val n_bits = sch.columns(coln).bits.length
    val q_bits = (0 to n_bits - 1).drop(drop_bits)
    util.BitUtils.group_values(q_bits, 0 to n_bits - 1).map(l => {
      val l2 = l.map(_.toInt)
      l2.min to (l2.max)
    })
  }

/*
  def interesting_cols(count_constraint: Int => Boolean) = {
    val col_idxs = dists.zipWithIndex.filter {
      case (v, _) => count_constraint(v.length)
    }.map(_._2)
  
    col_names(sch).zipWithIndex.filter{
      case (_, i) => col_idxs.contains(i) 
    }.map(_._1)
  }
*/
}


/*
import frontend._
import frontend.schema._
import frontend.Exploration._
import backend._
import core._
import core.solver.RationalTools._

val sch = new DynamicSchema
val R   = sch.read("/Users/ckoch/json/Shoppen.json")
val fc  = CBackend.b.mk(sch.n_bits, R.toIterator)
//val fc  = ScalaBackend.mk(sch.n_bits, R.toIterator)
val m   = RandomizedMaterializationScheme(sch.n_bits, .005, 1.02)
val dc  = new DataCube(m)
dc.build(fc)


import Exploration._

col_names(sch).foreach {println(_)}

val q = sch.columns("location.latitude").bits.toList


val dists = col_names(sch).map{ coln => dist(sch, dc, coln) } 

interesting_cols(x => x > 2 && x < 5)
interesting_cols(x => x > 100)

dist(sch, dc, "location.zipcode")


val q = sch.columns("location.name").bits.toList
val result = dc.naive_eval(q)
sch.decode_dim(q).zip(result).filter(_._2 > 1)

val q = sch.columns("location.address").bits.toList
sch.decode_dim(q).zip(result).filter(_._2 > 1)



import frontend._
val sch = schema.StaticSchema.mk(5)
sch.save("moo")
schema.Schema.load("moo")

*/


